package server

import (
	"context"
	"encoding/binary"
	"net"

	protocol "github.com/Aadithya-J/mini-kafka/internal/protocol"
)

func (s *Server) HandleConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-connCtx.Done()
		conn.Close()
	}()
	//will need in future maybe

	for {
		msg, err := readFrame(conn)
		if err != nil {
			return
		}
		resp, err := protocol.ProcessFrame(msg)
		if err != nil {
			return
		}
		size := len(resp)
		var sizeBuf [4]byte
		binary.BigEndian.PutUint32(sizeBuf[:], uint32(size))
		_, err = conn.Write(sizeBuf[:])
		_, err = conn.Write(resp)
		if err != nil {
			return
		}
	}
}

