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

func write16(w *bytes.Buffer, n int16) {
	err := binary.Write(w, binary.BigEndian, n)
	if err != nil {
		panic(err)
	}
}

func write32(w *bytes.Buffer, n int32) {
	err := binary.Write(w, binary.BigEndian, n)
	if err != nil {
		panic(err)
	}
}
