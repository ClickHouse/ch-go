package proto

//go:generate go run github.com/dmarkham/enumer -type ServerCode -trimprefix ServerCode -output server_code_gen.go

// ServerCode is sent by server to client.
type ServerCode byte

// Possible server codes.
const (
	ServerCodeHello        ServerCode = 0  // Server part of "handshake"
	ServerCodeData         ServerCode = 1  // data block (can be compressed)
	ServerCodeException    ServerCode = 2  // runtime exception
	ServerCodeProgress     ServerCode = 3  // query execution progress (bytes, lines)
	ServerCodePong         ServerCode = 4  // ping response (ClientPing)
	ServerCodeEndOfStream  ServerCode = 5  // end of stream
	ServerCodeProfile      ServerCode = 6  // profiling info
	ServerCodeTotals       ServerCode = 7  // packet with total values (can be compressed)
	ServerCodeExtremes     ServerCode = 8  // packet with minimums and maximums (can be compressed)
	ServerCodeTablesStatus ServerCode = 9  // response to TablesStatus
	ServerCodeLog          ServerCode = 10 // query execution system log
	ServerCodeTableColumns ServerCode = 11 // columns description
)

// Encode to buffer.
func (c ServerCode) Encode(b *Buffer) { b.PutByte(byte(c)) }

// Compressible reports whether message can be compressed.
func (c ServerCode) Compressible() bool {
	switch c {
	case ServerCodeData, ServerCodeTotals, ServerCodeExtremes:
		return true
	default:
		return false
	}
}
