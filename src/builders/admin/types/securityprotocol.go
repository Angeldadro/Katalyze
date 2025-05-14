package admin_types

type SecurityProtocol string

const (
	SecurityPLAINTEXT SecurityProtocol = "PLAINTEXT"
	SecuritySSL       SecurityProtocol = "SSL"
)

func IsValidSecurityProtocol(s SecurityProtocol) bool {
	switch s {
	case SecurityPLAINTEXT, SecuritySSL:
		return true
	default:
		return false
	}
}
