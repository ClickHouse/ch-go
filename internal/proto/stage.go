package proto

// Stage of query execution.
type Stage byte

// Encode to buffer.
func (s Stage) Encode(b *Buffer) { b.PutUVarInt(uint64(s)) }

//go:generate go run github.com/dmarkham/enumer -type Stage -trimprefix Stage -output stage_gen.go

// StageComplete is query complete.
const StageComplete Stage = 2
