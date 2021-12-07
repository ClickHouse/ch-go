package proto

type ClientData struct {
	TableName string
	Block     Block
}

func (c ClientData) EncodeAware(b *Buffer, revision int) {
	ClientCodeData.Encode(b)
	if FeatureTempTables.In(revision) {
		b.PutString(c.TableName)
	}
	c.Block.EncodeAware(b, revision)
}
