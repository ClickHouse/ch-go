package proto

//go:generate go run github.com/dmarkham/enumer -type ClientCode -trimprefix ClientCode -output client_code_gen.go

// ClientCode is sent from client to server.
type ClientCode byte

// Possible client codes.
const (
	ClientCodeHello           ClientCode = 0 // client part of "handshake"
	ClientCodeQuery           ClientCode = 1 // query start
	ClientCodeData            ClientCode = 2 // data block (can be compressed)
	ClientCodeCancel          ClientCode = 3 // query cancel
	ClientCodePing            ClientCode = 4 // ping request to server
	ClientTablesStatusRequest ClientCode = 5 // tables status request
)

// Encode to buffer.
func (c ClientCode) Encode(b *Buffer) { b.PutByte(byte(c)) }

// Compressible reports whether message can be compressed.
func (c ClientCode) Compressible() bool {
	switch c {
	case ClientCodeData:
		return true
	default:
		return false
	}
}
