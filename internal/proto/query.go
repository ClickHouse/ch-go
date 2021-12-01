package proto

type Query struct {
	ID          string
	Body        string
	Compression Compression
	Info        ClientInfo
	Settings    []Setting
}

type Setting struct {
	Key       string
	Value     string
	Important bool
}

func (s Setting) Encode(b *Buffer) {
	b.PutString(s.Key)
	b.PutBool(s.Important)
	b.PutString(s.Value)
}

func (q Query) EncodeAware(b *Buffer, revision int) {
	ClientCodeQuery.Encode(b)
	b.PutString(q.ID)
	if FeatureClientWriteInfo.In(revision) {
		q.Info.EncodeAware(b, revision)
	}
	if FeatureSettingsSerializedAsStrings.In(revision) {
		for _, s := range q.Settings {
			s.Encode(b)
		}
	}
	b.PutString("")

	if FeatureInterServerSecret.In(revision) {
		b.PutString("") // ?
	}

	StageComplete.Encode(b)
	q.Compression.Encode(b)

	b.PutString(q.Body)
}
