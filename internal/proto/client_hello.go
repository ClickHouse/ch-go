package proto

// ClientHello represents ClientCodeHello message.
type ClientHello struct {
	Name string

	Major int // client major version
	Minor int // client minor version

	// ProtocolVersion is TCP protocol version of client.
	//
	// Usually it is equal to the latest compatible server revinio.
	ProtocolVersion int

	Database string
	User     string
	Password string
}

// Encode to Buffer.
func (c ClientHello) Encode(b *Buffer) {
	ClientCodeHello.Encode(b)
	b.PutString(c.Name)
	b.PutInt(c.Major)
	b.PutInt(c.Minor)
	b.PutInt(c.ProtocolVersion)
	b.PutString(c.Database)
	b.PutString(c.User)
	b.PutString(c.Password)
}
