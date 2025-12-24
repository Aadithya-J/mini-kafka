package server

import (
	"context"
	"encoding/binary"
	"io"
	"net"

	protocol "github.com/Aadithya-J/mini-kafka/internal/protocol"
)

func (s *Server) HandleConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		msg, err := readFrame(conn)
		if err != nil {
			return
		}
		resp, err := protocol.ProcessFrame(msg)
		if err != nil {
			return
		}
		// in case of ack == 0
		if resp == nil {
			continue
		}
		size := len(resp)
		var sizeBuf [4]byte
		binary.BigEndian.PutUint32(sizeBuf[:], uint32(size))
		if err := writeAll(conn, sizeBuf[:]); err != nil {
			return
		}
		if err := writeAll(conn, resp); err != nil {
			return
		}
	}
}

func writeAll(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}
