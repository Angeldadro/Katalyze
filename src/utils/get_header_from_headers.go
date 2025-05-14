package utils

import "github.com/Angeldadro/Katalyze/src/types"

func GetHeaderFromHeaders(headers []types.Header, key string) types.Header {
	for _, header := range headers {
		if string(header.Key()) == key {
			return header
		}
	}
	return nil
}
