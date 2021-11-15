package proto

//go:generate go run github.com/dmarkham/enumer -type ClientCode -trimprefix Client -output client_code_gen.go

// ClientCode is sent from client to server.
type ClientCode byte

const (
	ClientHello  ClientCode = 0 // client part of "handshake"
	ClientQuery  ClientCode = 1 // query start
	ClientData   ClientCode = 2 // data block (can be compressed)
	ClientCancel ClientCode = 3 // query cancel
	ClientPing   ClientCode = 4 // ping request to server
)

// Compressible reports whether message can be compressed.
func (c ClientCode) Compressible() bool {
	switch c {
	case ClientData:
		return true
	default:
		return false
	}
}
