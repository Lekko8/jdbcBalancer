package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config Config
	config.readConfig()

	proxy := NewProxyServer(config)
	if err := proxy.Start(); err != nil {
		log.Fatalf("Failed to start proxy: %v", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Received shutdown signal")
	proxy.Stop()

}
