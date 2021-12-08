package proto

type ClientData struct {
	TableName string
	Block     Block
}

func (c ClientData) EncodeAware(b *Buffer, version int) {
	ClientCodeData.Encode(b)
	if FeatureTempTables.In(version) {
		b.PutString(c.TableName)
	}
	c.Block.EncodeAware(b, version)
}
