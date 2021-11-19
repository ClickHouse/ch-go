package proto

// Stage of query execution.
type Stage byte

// Encode to buffer.
func (s Stage) Encode(b *Buffer) { b.PutByte(byte(s)) }

// StageComplete is query complete.
const StageComplete Stage = 2
