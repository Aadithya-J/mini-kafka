package server

import (
	"encoding/binary"
	"io"
	"net"
	"fmt"
	"errors"
)

const maxFrameSize = 1 << 20 // 1 MB

func readFrame(conn net.Conn) ([]byte, error) {
    sizeBuf := make([]byte, 4)

    if _, err := io.ReadFull(conn, sizeBuf); err != nil {
        return nil, err
    }

    size := binary.BigEndian.Uint32(sizeBuf)

    if size == 0 {
        return nil, errors.New("empty frame")
    }

    if size > maxFrameSize {
        return nil, fmt.Errorf("frame too large: %d > %d", size, maxFrameSize)
    }

    msg := make([]byte, size)
    if _, err := io.ReadFull(conn, msg); err != nil {
        return nil, fmt.Errorf("read frame body: %w", err)
    }

    return msg, nil
}

func writeFrame(conn net.Conn, msg []byte) error {
    size := len(msg)

    if size == 0 {
        return errors.New("empty frame")
    }

    if size > maxFrameSize {
        return fmt.Errorf("frame too large: %d > %d", size, maxFrameSize)
    }

    var sizeBuf [4]byte
    binary.BigEndian.PutUint32(sizeBuf[:], uint32(size))

    if _, err := conn.Write(sizeBuf[:]); err != nil {
        return err
    }

    if _, err := conn.Write(msg); err != nil {
        return err
    }

    return nil
}


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


