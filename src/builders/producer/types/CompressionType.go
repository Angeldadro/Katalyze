package producer_types

type CompressionType string

const (
	CompressionTypeNone    CompressionType = "none"
	CompressionTypeGzip    CompressionType = "gzip"
	CompressionTypeSnappy  CompressionType = "snappy"
	CompressionTypeLz4     CompressionType = "lz4"
	CompressionTypeZstd    CompressionType = "zstd"
	CompressionTypeDefault CompressionType = "default"
)

func (c CompressionType) String() string {
	return string(c)
}
