package admin_types

type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionGzip   CompressionType = "gzip"
	CompressionSnappy CompressionType = "snappy"
	CompressionLz4    CompressionType = "lz4"
	CompressionZstd   CompressionType = "zstd"
)

func IsValidCompressionType(c CompressionType) bool {
	switch c {
	case CompressionNone, CompressionGzip, CompressionSnappy, CompressionLz4, CompressionZstd:
		return true
	default:
		return false
	}
}
