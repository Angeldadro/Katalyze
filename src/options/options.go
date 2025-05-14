package options

type Option struct {
	key   string
	value interface{}
}

func (h *Option) Key() string        { return h.key }
func (h *Option) Value() interface{} { return h.value }

func NewOption(key string, value interface{}) *Option {
	return &Option{key: key, value: value}
}
