package compress

import (
	"compress/gzip"
	"context"
	"mime"
	"net/http"
	"slices"

	"github.com/klauspost/compress/gzhttp"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/middlewares"
	"github.com/traefik/traefik/v2/pkg/tracing"
)

const (
	typeName = "Compress"
)

// Compress is a middleware that allows to compress the response.
type compress struct {
	next             http.Handler
	name             string
	excludes         []string
	minSize          int
	compressionLevel int
}

// New creates a new compress middleware.
func New(ctx context.Context, next http.Handler, conf dynamic.Compress, name string) (http.Handler, error) {
	log.FromContext(middlewares.GetLoggerCtx(ctx, name, typeName)).Debug("Creating middleware")

	excludes := []string{"application/grpc"}
	for _, v := range conf.ExcludedContentTypes {
		mediaType, _, err := mime.ParseMediaType(v)
		if err != nil {
			return nil, err
		}

		excludes = append(excludes, mediaType)
	}

	minSize := gzhttp.DefaultMinSize
	if conf.MinResponseBodyBytes > 0 {
		minSize = conf.MinResponseBodyBytes
	}

	compressionLevel := gzip.DefaultCompression
	if conf.CompressionLevel >= gzip.BestSpeed && conf.CompressionLevel <= gzip.BestCompression {
		compressionLevel = conf.CompressionLevel
	}

	return &compress{next: next, name: name, excludes: excludes, minSize: minSize, compressionLevel: compressionLevel}, nil
}

func (c *compress) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	mediaType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		log.FromContext(middlewares.GetLoggerCtx(context.Background(), c.name, typeName)).Debug(err)
	}

	if slices.Contains(c.excludes, mediaType) {
		c.next.ServeHTTP(rw, req)
	} else {
		ctx := middlewares.GetLoggerCtx(req.Context(), c.name, typeName)
		c.gzipHandler(ctx).ServeHTTP(rw, req)
	}
}

func (c *compress) GetTracingInformation() (string, ext.SpanKindEnum) {
	return c.name, tracing.SpanKindNoneEnum
}

func (c *compress) gzipHandler(ctx context.Context) http.Handler {
	wrapper, err := gzhttp.NewWrapper(
		gzhttp.ExceptContentTypes(c.excludes),
		gzhttp.CompressionLevel(c.compressionLevel),
		gzhttp.MinSize(c.minSize))
	if err != nil {
		log.FromContext(ctx).Error(err)
	}

	return wrapper(c.next)
}
