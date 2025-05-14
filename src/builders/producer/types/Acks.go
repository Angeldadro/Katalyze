package producer_types

type Acks string

const (
	AcksNone Acks = "0"
	AcksOne  Acks = "1"
	AcksAll  Acks = "-1"
)

func (a Acks) String() string {
	return string(a)
}
