package tcpstreamcompress

import (
	"fmt"
	"github.com/klauspost/compress/zstd"
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
}

func NewZStdDecompressor(conn tcp.WriteCloser, level zstd.EncoderLevel, dict []byte) tcp.WriteCloser {
	z := &zstdDecompressor{
		WriteCloser: conn,
	}
	var err error
	decoderOptions := make([]zstd.DOption, 0)
	if dict != nil {
		decoderOptions = append(decoderOptions, zstd.WithDecoderDicts(dict))
	}
	z.reader, err = zstd.NewReader(conn, decoderOptions...)
	if err != nil {
		panic(err)
	}
	encoderOptions := make([]zstd.EOption, 0)
	if dict != nil {
		encoderOptions = append(encoderOptions, zstd.WithEncoderDict(dict))
	}
	encoderOptions = append(encoderOptions, zstd.WithEncoderLevel(level))
	z.writer, err = zstd.NewWriter(conn, encoderOptions...)
	if err != nil {
		panic(err)
	}
	return z
}
func (z *zstdDecompressor) Read(p []byte) (n int, err error) {
	return z.reader.Read(p)
}
func (z *zstdDecompressor) Write(p []byte) (n int, err error) {
	n, err = z.writer.Write(p)
	// Send the zstd flush block to upstream
	err = z.writer.Flush()
	if err != nil {
		return 0, err
	}
	return n, err
}

func (z *zstdDecompressor) Close() error {
	z.writer.Close()
	z.reader.Close()
	return z.WriteCloser.Close()
}
func (z *zstdDecompressor) CloseWrite() error {
	z.writer.Close()
	return z.WriteCloser.CloseWrite()
}

// Take decompressed data from upstream and send it compressed
type zstdCompressor struct {
	tcp.WriteCloser

	compressor_r   *io.PipeReader
	decompressor_w *io.PipeWriter

	decompressor   *zstd.Decoder
	compressor     *zstd.Encoder
	writeWaitGroup sync.WaitGroup
	waitGroup      sync.WaitGroup
}

func NewZStdCompressor(conn tcp.WriteCloser, level zstd.EncoderLevel, dict []byte) tcp.WriteCloser {
	z := &zstdCompressor{
		WriteCloser:    conn,
		writeWaitGroup: sync.WaitGroup{},
		waitGroup:      sync.WaitGroup{},
	}

	var err error
	compressor_r, compressor_w := io.Pipe()
	encoderOptions := make([]zstd.EOption, 0)
	if dict != nil {
		encoderOptions = append(encoderOptions, zstd.WithEncoderDict(dict))
	}
	encoderOptions = append(encoderOptions, zstd.WithEncoderLevel(level))
	z.compressor, err = zstd.NewWriter(compressor_w, encoderOptions...)
	if err != nil {
		panic(err)
	}

	// TODO: I'm not particularly happy about these being goroutines, IMO they
	// should fit just fine into the Read/Write functions but it seemed to add
	// a lot of code overhead. Perhaps there is a library for it?
	z.waitGroup.Add(1)
	go func() {
		defer func() {
			compressor_w.CloseWithError(io.EOF)
			compressor_r.CloseWithError(io.EOF)
			z.compressor.Close()
			z.waitGroup.Done()
		}()

		tmp := make([]byte, 32*1024)
		for {
			// read from conn and write to compressor. Cannot use io.Copy because it will not flush
			n, err := conn.Read(tmp)
			fmt.Printf("Compressor read %d bytes: %s\n", n, tmp[0:n])
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}
			n, err = z.compressor.Write(tmp[0:n])
			if err != nil {
				return
			}
			// Have to flush each time to get the zstd flush block
			z.compressor.Flush()
		}
	}()

	decompressor_r, decompressor_w := io.Pipe()
	decoderOptions := make([]zstd.DOption, 0)
	if dict != nil {
		decoderOptions = append(decoderOptions, zstd.WithDecoderDicts(dict))
	}
	z.decompressor, err = zstd.NewReader(decompressor_r, decoderOptions...)
	if err != nil {
		panic(err)
	}
	z.writeWaitGroup.Add(1)
	go func() {
		defer func() {
			decompressor_w.CloseWithError(io.EOF)
			decompressor_r.CloseWithError(io.EOF)
			z.decompressor.Close()
			z.writeWaitGroup.Done()
		}()
		io.Copy(conn, z.decompressor)
	}()

	z.compressor_r = compressor_r
	z.decompressor_w = decompressor_w

	return z
}
func (z *zstdCompressor) Read(p []byte) (n int, err error) {
	return z.compressor_r.Read(p)
}
func (z *zstdCompressor) Write(p []byte) (n int, err error) {
	return z.decompressor_w.Write(p)
}

func (z *zstdCompressor) Close() error {
	z.decompressor_w.CloseWithError(io.EOF)
	z.writeWaitGroup.Wait()

	z.WriteCloser.SetReadDeadline(time.Now())
	z.waitGroup.Wait()

	return z.WriteCloser.Close()
}
func (z *zstdCompressor) CloseWrite() error {
	z.decompressor_w.CloseWithError(io.EOF)
	z.writeWaitGroup.Wait()

	return z.WriteCloser.CloseWrite()
}
