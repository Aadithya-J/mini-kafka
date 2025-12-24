package main

import (
	"log"
	"net"

	"github.com/Aadithya-J/mini-kafka/internal/config"
)

func main() {
	cfg := config.Load()

	addr := ":" + cfg.Port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("Error Listening on %s: ", cfg.Port, err)
	}
	log.Printf("hello")
	defer lis.Close()

	log.Printf("server listening on port %s", cfg.Port)

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("Accept failed : %s")
			continue
		}
		go handleConn(conn)
	}

}


func main2() {
	cfg := config.Load()
	srv := server.New()
	err := srv.Start(cfg)
	if err != nil {
		log.Printf("Error starting server:",err)
	}
}