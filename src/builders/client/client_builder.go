package client_builder

import (
	"fmt"

	"github.com/Angeldadro/Katalyze/src/client"
	"github.com/Angeldadro/Katalyze/src/types"
)

type ClientBuilder struct {
	ClientId    string
	AdminClient types.AdminClient
}

func NewClientBuilder() *ClientBuilder {
	return &ClientBuilder{}
}

func (b *ClientBuilder) SetClientId(clientId string) *ClientBuilder {
	b.ClientId = clientId
	return b
}
func (b *ClientBuilder) SetAdminClient(adminClientInterface types.AdminClient) *ClientBuilder {
	b.AdminClient = adminClientInterface
	return b
}

func (b *ClientBuilder) Build() (*client.Client, error) {
	if b.AdminClient == nil {
		return nil, fmt.Errorf("adminClientInterface is nil")
	}
	if b.ClientId == "" {
		return nil, fmt.Errorf("clientId is empty")
	}
	kc := client.NewClient(b.ClientId, b.AdminClient)
	return kc, nil
}
