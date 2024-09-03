package tcpstreamcompress

import (
	"context"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/tcp"
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
		// will write to decompressor(compresses data) -> compressor(decompresses data) -> client -> server
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

	// Pipeline is now decompressor ⇌ echo function
	decompressor, err := New(context.Background(), next, decompressorConfig, "traefikTest2")
	require.NoError(t, err)

	compressorConfig := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "best",
		Upstream:  false,
	}

	// Pipeline is now compressor ⇌ decompressor ⇌ echo function
	compressor, err := New(context.Background(), decompressor, compressorConfig, "traefikTest")
	require.NoError(t, err)

	server, client := net.Pipe()

	go func() {
		// Pipeline is now server ⇌ client ⇌ compressor ⇌ decompressor ⇌ echo function
		compressor.ServeTCP(&contextWriteCloser{client, addr{"10.10.10.10"}})
	}()

	// Read the data from the server. The data is originating from echo function -> decompressor(compresses data) -> compressor(decompresses data) -> client -> server
	read, err := io.ReadAll(server)
	require.NoError(t, err)

	assert.Equal(t, message, string(read))

	err = server.Close()
	require.NoError(t, err)
}

func TestStreamCompress_ServeTCPCompression(t *testing.T) {
	next := tcp.HandlerFunc(func(conn tcp.WriteCloser) {
		// will write to decompressor(compresses data) -> client -> server.
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

	// Pipeline is now decompressor ⇌ echo function
	decompressor, err := New(context.Background(), next, decompressorConfig, "traefikTest3")
	require.NoError(t, err)

	server, client := net.Pipe()

	go func() {
		// Pipeline is now server ⇌ client ⇌ decompressor ⇌ echo function
		decompressor.ServeTCP(&contextWriteCloser{client, addr{"10.10.10.10"}})
	}()

	// Read the compressed data from the server. The data is originating from echo function -> decompressor(compresses data) -> client -> server
	readCompressed, err := io.ReadAll(server)
	require.NoError(t, err)

	assert.NotEqual(t, []byte(message), readCompressed)

	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)

	read, err := decoder.DecodeAll(readCompressed, nil)

	assert.Equal(t, message, string(read))

	err = server.Close()
	require.NoError(t, err)

	decoder.Close()
}

func TestStreamCompress_ServeTCPDecompression(t *testing.T) {
	compressor, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	compressedMessage := compressor.EncodeAll([]byte(message), nil)
	assert.NotEqual(t, []byte(message), compressedMessage)

	err = compressor.Close()
	require.NoError(t, err)

	next := tcp.HandlerFunc(func(conn tcp.WriteCloser) {
		// will write compressed data to decompressor(decompresses data) -> client -> server.
		write, err := conn.Write(compressedMessage)
		require.NoError(t, err)
		assert.Equal(t, len(compressedMessage), write)

		time.Sleep(100 * time.Millisecond)

		err = conn.Close()
		require.NoError(t, err)
	})

	decompressorConfig := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "best",
		Upstream:  false,
	}

	// Pipeline is now decompressor ⇌ echo function
	decompressor, err := New(context.Background(), next, decompressorConfig, "traefikTest3")
	require.NoError(t, err)

	server, client := net.Pipe()

	go func() {
		// Pipeline is now server ⇌ client ⇌ decompressor ⇌ echo function
		decompressor.ServeTCP(&contextWriteCloser{client, addr{"10.10.10.10"}})
	}()

	// Read the decompressed data from the server. The data is originating from echo function -> decompressor(decompresses data) -> client -> server
	read, err := io.ReadAll(server)
	require.NoError(t, err)

	assert.Equal(t, message, string(read))

	err = server.Close()
	require.NoError(t, err)
}

func layeredCompressor(next tcp.Handler, layers int, config dynamic.TCPStreamCompress) tcp.Handler {
	config.Upstream = true
	for i := 0; i < (layers * 2); i++ {
		next, _ = New(context.Background(), next, config, "traefikTest")
		config.Upstream = !config.Upstream
	}
	return next
}

func BenchmarkLayeredStreamCompress(b *testing.B) {
	numberOfLayers := 100
	dataSize := 50 * 1024 // 50 KB
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	next := tcp.HandlerFunc(func(conn tcp.WriteCloser) {
		write, err := conn.Write(data)
		require.NoError(b, err)
		assert.Equal(b, len(data), write)

		time.Sleep(100 * time.Millisecond)

		err = conn.Close()
		require.NoError(b, err)
	})

	config := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "best",
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		layeredHandler := layeredCompressor(next, numberOfLayers, config)

		server, client := net.Pipe()

		go func() {
			layeredHandler.ServeTCP(&contextWriteCloser{client, addr{"10.10.10.10"}})
		}()

		read, err := io.ReadAll(server)
		require.NoError(b, err)

		assert.Equal(b, data, read)

		err = server.Close()
		require.NoError(b, err)
	}

	b.StopTimer()
}

func BenchmarkStreamCompress(b *testing.B) {
	const defaultIterations = 10000
	if b.N == 1 { // Check if b.N is the default value set by the framework
		b.N = defaultIterations
	}

	dataSize := 50 * 1024 // 50 KB
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	compressor, err := zstd.NewWriter(nil)
	require.NoError(b, err)

	compressedMessage := compressor.EncodeAll(data, nil)
	assert.NotEqual(b, data, compressedMessage)

	err = compressor.Close()
	require.NoError(b, err)

	next := tcp.HandlerFunc(func(conn tcp.WriteCloser) {
		write, err := conn.Write(compressedMessage)
		require.NoError(b, err)
		assert.Equal(b, len(compressedMessage), write)

		time.Sleep(100 * time.Millisecond)

		err = conn.Close()
		require.NoError(b, err)
	})

	decompressorConfig := dynamic.TCPStreamCompress{
		Algorithm: "zstd",
		Level:     "best",
		Upstream:  false,
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			decompressor, err := New(context.Background(), next, decompressorConfig, "traefikTest3")
			require.NoError(b, err)

			server, client := net.Pipe()

			go func() {
				decompressor.ServeTCP(&contextWriteCloser{client, addr{"10.10.10.10"}})
			}()

			read, err := io.ReadAll(server)
			require.NoError(b, err)

			assert.Equal(b, data, read)

			err = server.Close()
			require.NoError(b, err)
		}
	})

	b.StopTimer()
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
