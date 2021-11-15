package proto

//go:generate go run github.com/dmarkham/enumer -type ServerCode -trimprefix Server -output server_code_gen.go

// ServerCode is sent by server to client.
type ServerCode byte

const (
	ServerHello        ServerCode = 0  // Server part of "handshake"
	ServerData         ServerCode = 1  // data block (can be compressed)
	ServerException    ServerCode = 2  // runtime exception
	ServerProgress     ServerCode = 3  // query execution progress (bytes, lines)
	ServerPong         ServerCode = 4  // ping response
	ServerEOF          ServerCode = 5  // end of stream
	ServerProfile      ServerCode = 6  // profiling info
	ServerTotals       ServerCode = 7  // packet with total values (can be compressed)
	ServerExtremes     ServerCode = 8  // packet with minimums and maximums (can be compressed)
	ServerTablesStatus ServerCode = 9  // response to TablesStatus
	ServerLog          ServerCode = 10 // query execution system log
)

// Compressible reports whether message can be compressed.
func (s ServerCode) Compressible() bool {
	switch s {
	case ServerData, ServerTotals, ServerExtremes:
		return true
	default:
		return false
	}
}
