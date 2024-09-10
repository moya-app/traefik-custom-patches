package tcpstreamcompress

import (
	"bytes"
	"context"
	"github.com/klauspost/compress/zstd"
	"github.com/traefik/traefik/v3/pkg/middlewares"
	"github.com/traefik/traefik/v3/pkg/tcp"
	"io"
	"sync"
	"time"
)

// Take compressed data from upstream and send it plain to backend
type zstdDecompressor struct {
	tcp.WriteCloser

	reader *zstd.Decoder
	writer *zstd.Encoder

	mu  sync.Mutex
	muW sync.Mutex
}

func NewZStdDecompressor(conn tcp.WriteCloser, level zstd.EncoderLevel, dict []byte) tcp.WriteCloser {
	z := &zstdDecompressor{
		WriteCloser: conn,
	}

	var err error
	var decoderOptions []zstd.DOption
	encoderOptions := []zstd.EOption{zstd.WithEncoderLevel(level)}

	if dict != nil {
		decoderOptions = append(decoderOptions, zstd.WithDecoderDicts(dict))
		encoderOptions = append(encoderOptions, zstd.WithEncoderDict(dict))
	}

	z.reader, err = zstd.NewReader(conn, decoderOptions...)
	if err != nil {
		panic(err)
	}
	z.writer, err = zstd.NewWriter(conn, encoderOptions...)
	if err != nil {
		panic(err)
	}
	return z
}
func (z *zstdDecompressor) Read(p []byte) (n int, err error) {
	defer z.mu.Unlock()
	z.mu.Lock()
	return z.reader.Read(p)
}
func (z *zstdDecompressor) Write(p []byte) (n int, err error) {
	defer z.muW.Unlock()
	z.muW.Lock()
	n, err = z.writer.Write(p)
	// Send the zstd flush block to upstream
	err = z.writer.Flush()
	if err != nil {
		return 0, err
	}
	return n, err
}

func (z *zstdDecompressor) Close() error {
	z.WriteCloser.SetDeadline(time.Now().Add(10 * time.Millisecond))
	defer z.mu.Unlock()
	defer z.muW.Unlock()
	z.mu.Lock()
	z.muW.Lock()

	writerErr := z.writer.Close()
	defer z.reader.Close()
	err := z.WriteCloser.Close()
	if writerErr != nil && err == nil {
		return writerErr
	}
	return err
}
func (z *zstdDecompressor) CloseWrite() error {
	z.WriteCloser.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	defer z.muW.Unlock()
	z.muW.Lock()

	writerErr := z.writer.Close()
	err := z.WriteCloser.CloseWrite()
	if writerErr != nil && err == nil {
		return writerErr
	}
	return err
}

// Take decompressed data from upstream and send it compressed

type zstdCompressor struct {
	tcp.WriteCloser

	reader *zstd.Decoder
	writer *zstd.Encoder

	readBuffer bytes.Buffer

	writerW        *io.PipeWriter
	writeWaitGroup sync.WaitGroup
}

func NewZStdCompressor(conn tcp.WriteCloser, level zstd.EncoderLevel, dict []byte) tcp.WriteCloser {
	z := &zstdCompressor{
		WriteCloser: conn,
	}

	logger := middlewares.GetLogger(context.Background(), "ZStdCompressor", typeName)

	var err error
	encoderOptions := []zstd.EOption{zstd.WithEncoderLevel(level)}
	var decoderOptions []zstd.DOption

	if dict != nil {
		encoderOptions = append(encoderOptions, zstd.WithEncoderDict(dict))
		decoderOptions = append(decoderOptions, zstd.WithDecoderDicts(dict))
	}

	z.writer, err = zstd.NewWriter(&z.readBuffer, encoderOptions...)
	if err != nil {
		panic(err)
	}

	writerR, writerW := io.Pipe()
	z.writerW = writerW
	z.reader, err = zstd.NewReader(writerR, decoderOptions...)
	if err != nil {
		panic(err)
	}

	z.writeWaitGroup.Add(1)
	go func() {
		defer func() {
			writerW.CloseWithError(io.EOF)
			writerR.CloseWithError(io.EOF)
			z.reader.Close()
			z.writeWaitGroup.Done()
		}()
		n, err := z.reader.WriteTo(conn)
		if err != nil && err != io.EOF {
			logger.Error().Msgf("Error writing to conn after writing %d bytes: %v", n, err)
		}
	}()

	return z
}

func (z *zstdCompressor) Read(p []byte) (n int, err error) {
	if z.readBuffer.Len() == 0 {
		const ChuckSize = 4 * 1024
		var uncompressedData [ChuckSize]byte
		n, err := z.WriteCloser.Read(uncompressedData[:])
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n > 0 {
			_, connEr := z.writer.Write(uncompressedData[:n])
			if connEr != nil {
				return 0, connEr
			}
			// Force a flush to trigger a send of the compressed stanza/data downstream, otherwise it could hang
			connEr = z.writer.Flush()
			if connEr != nil {
				return 0, connEr
			}
		}
		if err == io.EOF && z.readBuffer.Len() == 0 {
			return 0, io.EOF
		}
	}
	return z.readBuffer.Read(p)
}

func (z *zstdCompressor) Write(p []byte) (n int, err error) {
	n, err = z.writerW.Write(p)
	if err != nil {
		return 0, err
	}

	return n, err
}

func (z *zstdCompressor) Close() error {
	z.writerW.CloseWithError(io.EOF)
	z.writeWaitGroup.Wait()

	writerErr := z.writer.Close()
	err := z.WriteCloser.Close()
	if writerErr != nil && err == nil {
		return writerErr
	}
	return err
}

func (z *zstdCompressor) CloseWrite() error {
	z.writerW.CloseWithError(io.EOF)
	z.writeWaitGroup.Wait()

	return z.WriteCloser.CloseWrite()
}
