package state

import (
	"io"
)

func (t *Command) Marshal(w io.Writer) {
	var b [1]byte
	// bs := b[:1071]
	b[0] = byte(t.Op)
	bs := b[:1]
	w.Write(bs)
	// bs = b[:8]
	// binary.LittleEndian.PutUint64(bs, uint64(t.K))
	bs = []byte(t.K)
	w.Write(bs)
	// binary.LittleEndian.PutUint64(bs, uint64(t.V))
	bs = []byte(t.V)
	w.Write(bs)
}

func (t *Command) Unmarshal(r io.Reader) error {
	var b [1071]byte
	bs := b[:1]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	t.Op = Operation(b[0])
	bs = b[:23]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	t.K = Key(bs)
	bs = b[:1070]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	t.V = Value(bs)
	return nil
}

func (t *Key) Marshal(w io.Writer) {
	// var b [8]byte
	// bs := b[:8]
	bs := []byte(*t)
	// binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
}

func (t *Value) Marshal(w io.Writer) {
	// var b [8]byte
	// bs := b[:8]
	// binary.LittleEndian.PutUint64(bs, uint64(*t))
	bs := []byte(*t)
	w.Write(bs)
}

func (t *Key) Unmarshal(r io.Reader) error {
	var b [24]byte
	bs := b[:23]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Key(bs)
	return nil
}

func (t *Value) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:1]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Value(bs)
	return nil
}
