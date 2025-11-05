package mysqliface

import (
	"io"
	"log"
	"net"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/fulldump/inceptiondb/service"
)

type Server struct {
	addr     string
	version  string
	user     string
	password string

	svc service.Servicer

	listener  net.Listener
	closeOnce sync.Once
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

func NewServer(addr, version, user, password string, svc service.Servicer) *Server {
	return &Server{
		addr:     addr,
		version:  version,
		user:     user,
		password: password,
		svc:      svc,
		stopCh:   make(chan struct{}),
	}
}

func (s *Server) Start() error {
	if s.addr == "" {
		return nil
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()

	log.Printf("mysql interface listening on %s", s.addr)

	return nil
}

func (s *Server) Stop() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.stopCh)
		if s.listener != nil {
			err = s.listener.Close()
		}
	})

	s.wg.Wait()

	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
			}

			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}

			log.Printf("mysql interface accept error: %v", err)
			return
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()

	defer conn.Close()

	provider := server.NewInMemoryProvider()
	provider.AddUser(s.user, s.password)

	mysqlServer := server.NewServer(s.version, mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)

	handler := NewHandler(s.svc, s.version)

	mysqlConn, err := server.NewCustomizedConn(conn, mysqlServer, provider, handler)
	if err != nil {
		log.Printf("mysql interface handshake error: %v", err)
		return
	}
	defer func() {
		if mysqlConn != nil && mysqlConn.Conn != nil {
			mysqlConn.Close()
		}
	}()

	for {
		if err := mysqlConn.HandleCommand(); err != nil {
			if err == io.EOF || mysqlConn.Closed() {
				return
			}
			log.Printf("mysql interface command error: %v", err)
			return
		}
	}
}
