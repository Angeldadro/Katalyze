package consumer_types

type AutoOffsetReset string

const (
	AutoOffsetResetEarliest AutoOffsetReset = "earliest"
	AutoOffsetResetLatest   AutoOffsetReset = "latest"
)
