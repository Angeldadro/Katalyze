package utils

import (
	"github.com/Angeldadro/Katalyze/src/options"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func MapConfigMapToOptions(config *kafka.ConfigMap) []types.Option {
	len := len(*config)
	options_arr := make([]types.Option, len)
	for i, v := range *config {
		options_arr = append(options_arr, options.NewOption(i, v))
	}
	return options_arr
}
