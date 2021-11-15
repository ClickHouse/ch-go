package proto

// ClientHello represents ClientCodeHello message.
type ClientHello struct {
	Name     string
	Major    int
	Minor    int
	Revision int
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
	b.PutInt(c.Revision)
	b.PutString(c.Database)
	b.PutString(c.User)
	b.PutString(c.Password)
}
