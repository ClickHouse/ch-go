package proto

type Stage byte

func (s Stage) Encode(b *Buffer) { b.PutByte(byte(s)) }

const StageComplete Stage = 2
