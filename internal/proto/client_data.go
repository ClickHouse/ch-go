package proto

type Block struct {
	Info BlockInfo

	Columns int
	Rows    int
}

func (b Block) EncodeAware(buf *Buffer, revision int) {
	if FeatureBlockInfo.In(revision) {
		b.Info.Encode(buf)
	}

	buf.PutInt(b.Columns)
	buf.PutInt(b.Rows)

	// TODO: Write columns and rows data
}

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
