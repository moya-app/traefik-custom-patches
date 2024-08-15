package tcpstreamcompress

import (
	"bytes"
	"github.com/klauspost/compress/zstd"
	"github.com/traefik/traefik/v3/pkg/tcp"
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

	reader *zstd.Decoder
	writer *zstd.Encoder

	readBuffer  bytes.Buffer
	writeBuffer bytes.Buffer
	chuckSize   int
}

func NewZStdCompressor(conn tcp.WriteCloser, level zstd.EncoderLevel, dict []byte) tcp.WriteCloser {
	z := &zstdCompressor{
		WriteCloser: conn,
		chuckSize:   4 * 1024,
	}

	var err error
	encoderOptions := []zstd.EOption{zstd.WithEncoderLevel(level), zstd.WithZeroFrames(true)}
	decoderOptions := []zstd.DOption{}

	if dict != nil {
		encoderOptions = append(encoderOptions, zstd.WithEncoderDict(dict))
		decoderOptions = append(decoderOptions, zstd.WithDecoderDicts(dict))
	}

	z.writer, err = zstd.NewWriter(nil, encoderOptions...)
	if err != nil {
		panic(err)
	}

	z.reader, err = zstd.NewReader(nil, decoderOptions...)
	if err != nil {
		panic(err)
	}

	return z
}

func (z *zstdCompressor) Read(p []byte) (n int, err error) {
	if z.readBuffer.Len() == 0 {
		unCompressedData := make([]byte, z.chuckSize)
		n, err = z.WriteCloser.Read(unCompressedData)
		if err != nil {
			return 0, err
		}

		compressedData := z.writer.EncodeAll(unCompressedData[:n], make([]byte, 0))
		z.readBuffer.Write(compressedData)
	}

	return z.readBuffer.Read(p)
}

func (z *zstdCompressor) Write(p []byte) (n int, err error) {
	n, err = z.writeBuffer.Write(p)
	if err != nil {
		return 0, err
	}

	if z.writeBuffer.Len() > z.chuckSize {
		err = z.Flush()
		if err != nil {
			return 0, err
		}
	}

	return n, err
}

func (z *zstdCompressor) Flush() error {
	if z.writeBuffer.Len() == 0 {
		return nil
	}
	compressedData := z.writeBuffer.Bytes()
	z.writeBuffer.Reset()
	decompressedData, err := z.reader.DecodeAll(compressedData, make([]byte, 0, z.chuckSize))
	if err != nil {
		return err
	}

	_, err = z.WriteCloser.Write(decompressedData)
	if err != nil {
		return err
	}

	return nil
}

func (z *zstdCompressor) Close() error {
	z.Flush()
	z.writer.Close()
	z.reader.Close()
	return z.WriteCloser.Close()
}

func (z *zstdCompressor) CloseWrite() error {
	z.Flush()
	z.writer.Close()
	return z.WriteCloser.CloseWrite()
}
