package tcpstreamcompress

import (
	"fmt"
	"github.com/DataDog/zstd"
	"github.com/traefik/traefik/v2/pkg/tcp"
	"io"
)

// Take compressed data from upstream and send it plain to backend
type zstdDecompressor struct {
	tcp.WriteCloser

	reader io.ReadCloser
	writer *zstd.Writer
}

func NewZStdDecompressor(conn tcp.WriteCloser, level int, dict []byte) tcp.WriteCloser {
	z := &zstdDecompressor{
		WriteCloser: conn,
	}
	z.reader = zstd.NewReaderDict(conn, dict)
	z.writer = zstd.NewWriterLevelDict(conn, level, dict)
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
func (z *zstdDecompressor) cleanup() error {
	// TODO: These may both return error so we should probably return to the caller if they do, but also clean up and keep the socket
	return z.reader.Close()
	// TODO: Crashes?
	//z.writer.Close()
}

func (z *zstdDecompressor) Close() error {
	err := z.cleanup()

	if err != nil {
		z.WriteCloser.Close()
		return err
	}
	return z.WriteCloser.Close()
}
func (z *zstdDecompressor) CloseWrite() error {
	err := z.cleanup()
	if err != nil {
		z.WriteCloser.CloseWrite()
		return err
	}
	return z.WriteCloser.CloseWrite()
}

// Take decompressed data from upstream and send it compressed
type zstdCompressor struct {
	tcp.WriteCloser

    compressor_r *io.PipeReader
    decompressor_w *io.PipeWriter

	decompressor io.ReadCloser
	compressor *zstd.Writer
}

func NewZStdCompressor(conn tcp.WriteCloser, level int, dict []byte) tcp.WriteCloser {
	z := &zstdCompressor{
		WriteCloser: conn,
	}

    compressor_r, compressor_w := io.Pipe()
    z.compressor = zstd.NewWriterLevelDict(compressor_w, level, dict)
    // TODO: I'm not particularly happy about these being goroutines, IMO they
    // should fit just fine into the Read/Write functions but it seemed to add
    // a lot of code overhead. Perhaps there is a library for it?
    go func() {
        defer compressor_w.Close()
        defer compressor_r.Close()

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
    z.decompressor = zstd.NewReaderDict(decompressor_r, dict)
    go func() {
        defer decompressor_w.Close()
        defer decompressor_r.Close()
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
func (z *zstdCompressor) cleanup() error {
	// TODO: These may both return error so we should probably return to the caller if they do, but also clean up and keep the socket
	return z.decompressor.Close()
	// TODO: This Crashes
	//z.compressor.Close()
}

func (z *zstdCompressor) Close() error {
	err := z.cleanup()

	if err != nil {
		z.WriteCloser.Close()
		return err
	}
	return z.WriteCloser.Close()
}
func (z *zstdCompressor) CloseWrite() error {
	err := z.cleanup()
	if err != nil {
		z.WriteCloser.CloseWrite()
		return err
	}
	return z.WriteCloser.CloseWrite()
}
