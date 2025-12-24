package server

import (
	"bytes"
	"encoding/binary"
)

func writeInt16(w *bytes.Buffer, n int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(n))
	w.Write(b[:])
}

func writeInt32(w *bytes.Buffer, n int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(n))
	w.Write(b[:])
}

func writeInt64(w *bytes.Buffer, n int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	w.Write(b[:])
}
