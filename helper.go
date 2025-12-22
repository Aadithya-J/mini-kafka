package main

import (
	"bytes"
	"encoding/binary"
	"io"
)

func readInt16(r io.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func readInt32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func readBytes(r io.Reader, n int32) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

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
