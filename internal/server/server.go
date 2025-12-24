package server

import (
	"context"
	"errors"
	"net"
	"sync"
)

type Server struct {
	mu      sync.Mutex
	running bool
	lis     net.Listener
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewServer() *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Server) Start(addr string) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("server already running")
	}
	s.running = true
	s.mu.Unlock()

	lis, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}
	s.lis = lis
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConn(s.ctx, conn)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return errors.New("server not running")
	}
	s.running = false
	s.mu.Unlock()
	s.cancel()
	if s.lis != nil {
		s.lis.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
