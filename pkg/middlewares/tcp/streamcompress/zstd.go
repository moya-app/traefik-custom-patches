package tcpstreamcompress

import (
	"fmt"
	"github.com/DataDog/zstd"
	"github.com/traefik/traefik/v2/pkg/tcp"
	"io"
)

type zstdConnection struct {
	tcp.WriteCloser

	reader io.ReadCloser
	writer *zstd.Writer
}

func NewZStdConnection(conn tcp.WriteCloser, level int, dict []byte) tcp.WriteCloser {
	z := &zstdConnection{
		WriteCloser: conn,
	}
	z.reader = zstd.NewReaderDict(conn, dict)
	z.writer = zstd.NewWriterLevelDict(conn, level, dict)
	return z
}
func (z *zstdConnection) Read(p []byte) (n int, err error) {
	n, err = z.reader.Read(p)
	fmt.Printf("Read %d bytes: %s\n", n, p)
	return n, err
}
func (z *zstdConnection) Write(p []byte) (n int, err error) {
	n, err = z.writer.Write(p)
	fmt.Printf("Wrote %d bytes: %s\n", n, p)
	if err != nil {
		return n, err
	}
	// Send the zstd flush block to upstream
	err = z.writer.Flush()
	return n, err
}
func (z *zstdConnection) cleanup() error {
	// TODO: These both return error so we should probably return to the caller if they do, but also clean up and keep the socket
	return z.reader.Close()
	// TODO: Crashes?
	//z.writer.Close()
}

func (z *zstdConnection) Close() error {
	err := z.cleanup()

	if err != nil {
		z.WriteCloser.Close()
		return err
	}
	return z.WriteCloser.Close()
}
func (z *zstdConnection) CloseWrite() error {
	err := z.cleanup()
	if err != nil {
		z.WriteCloser.CloseWrite()
		return err
	}
	return z.WriteCloser.CloseWrite()
}
