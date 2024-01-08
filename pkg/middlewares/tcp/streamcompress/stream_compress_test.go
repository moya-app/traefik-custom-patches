package tcpstreamcompress

import (
	"context"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"github.com/traefik/traefik/v2/pkg/tcp"
	"io"
	"net"
	"testing"
	"time"
)

const message = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua"

type mockHandler struct{}

func (m *mockHandler) ServeTCP(conn tcp.WriteCloser) {}

func TestNewStreamCompressWithValidZstdAlgorithm(t *testing.T) {
	config := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "default",
		Upstream:  false,
	}
	_, err := New(context.Background(), &mockHandler{}, config, "test")
	require.NoError(t, err)
}

func TestNewStreamCompressWithInvalidAlgorithm(t *testing.T) {
	config := dynamic.TCPStreamCompress{
		Algorithm: "invalid",
		Level:     "default",
		Upstream:  false,
	}
	_, err := New(context.Background(), &mockHandler{}, config, "test")
	assert.Error(t, err)
}

func TestNewStreamCompressWithInvalidLevel(t *testing.T) {
	config := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "invalid",
		Upstream:  false,
	}
	_, err := New(context.Background(), &mockHandler{}, config, "test")
	assert.Error(t, err)
}

func TestNewStreamCompressWithInvalidDictionary(t *testing.T) {
	config := dynamic.TCPStreamCompress{
		Algorithm:  "zstd",
		Level:      "default",
		Upstream:   false,
		Dictionary: "invalid",
	}
	_, err := New(context.Background(), &mockHandler{}, config, "test")
	assert.Error(t, err)
}

func TestStreamCompress_ServeTCP(t *testing.T) {
	next := tcp.HandlerFunc(func(conn tcp.WriteCloser) {
		write, err := conn.Write([]byte(message))
		// sleep for a bit to ensure the flush block is sent
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, len(message), write)

		err = conn.Close()
		require.NoError(t, err)
	})

	decompressorConfig := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "best",
		Upstream:  true,
	}

	decompressor, err := New(context.Background(), next, decompressorConfig, "traefikTest2")
	require.NoError(t, err)

	compressorConfig := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "best",
		Upstream:  false,
	}

	compressor, err := New(context.Background(), decompressor, compressorConfig, "traefikTest")
	require.NoError(t, err)

	server, client := net.Pipe()

	go func() {
		compressor.ServeTCP(&contextWriteCloser{client, addr{"10.10.10.10"}})
	}()

	read, err := io.ReadAll(server)
	require.NoError(t, err)

	assert.Equal(t, message, string(read))

	err = server.Close()
	require.NoError(t, err)
}

func TestStreamCompress_ServeTCPCompression(t *testing.T) {
	next := tcp.HandlerFunc(func(conn tcp.WriteCloser) {
		write, err := conn.Write([]byte(message))
		require.NoError(t, err)
		assert.Equal(t, len(message), write)

		err = conn.Close()
		require.NoError(t, err)
	})

	decompressorConfig := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "best",
		Upstream:  true,
	}

	decompressor, err := New(context.Background(), next, decompressorConfig, "traefikTest3")
	require.NoError(t, err)

	server, client := net.Pipe()

	go func() {
		decompressor.ServeTCP(&contextWriteCloser{client, addr{"10.10.10.10"}})
	}()

	serverDecompressor, err := zstd.NewReader(server)
	require.NoError(t, err)

	read, err := io.ReadAll(serverDecompressor)
	require.NoError(t, err)

	assert.Equal(t, message, string(read))

	err = server.Close()
	require.NoError(t, err)

	serverDecompressor.Close()
}

func TestStreamCompress_ServeTCPDecompression(t *testing.T) {
	next := tcp.HandlerFunc(func(conn tcp.WriteCloser) {
		compressor, err := zstd.NewWriter(conn)
		require.NoError(t, err)

		write, err := compressor.Write([]byte(message))
		require.NoError(t, err)
		assert.Equal(t, len(message), write)

		err = compressor.Flush()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		err = conn.Close()
		require.NoError(t, err)
	})

	decompressorConfig := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "best",
		Upstream:  false,
	}

	decompressor, err := New(context.Background(), next, decompressorConfig, "traefikTest3")
	require.NoError(t, err)

	server, client := net.Pipe()

	go func() {
		decompressor.ServeTCP(&contextWriteCloser{client, addr{"10.10.10.10"}})
	}()

	read, err := io.ReadAll(server)
	require.NoError(t, err)

	assert.Equal(t, message, string(read))

	err = server.Close()
	require.NoError(t, err)
}

type contextWriteCloser struct {
	net.Conn
	addr
}

type addr struct {
	remoteAddr string
}

func (a addr) Network() string {
	panic("implement me")
}

func (a addr) String() string {
	return a.remoteAddr
}

func (c contextWriteCloser) CloseWrite() error {
	panic("implement me")
}

func (c contextWriteCloser) RemoteAddr() net.Addr {
	return c.addr
}

func (c contextWriteCloser) Context() context.Context {
	return context.Background()
}
