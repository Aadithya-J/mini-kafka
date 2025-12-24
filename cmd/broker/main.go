package main

import (
	"log"

	"github.com/Aadithya-J/mini-kafka/internal/config"
	"github.com/Aadithya-J/mini-kafka/internal/server"
)

func main() {
	cfg := config.Load()
	srv := server.NewServer()
	err := srv.Start(cfg)
	if err != nil {
		log.Printf("Error starting server: %v", err)
	}
	select {}
}
