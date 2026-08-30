package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/auth"
	"github.com/jaipreethtiruvaipati/redis-clone/app/server"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	addr := flag.String("addr", "0.0.0.0:6379", "TCP listen address")
	requirePass := flag.String("requirepass", "", "require this password for the default user")
	flag.Parse()
	if *requirePass != "" {
		auth.DefaultUser().SetPassword(*requirePass)
	}
	s := server.New(*addr)
	startErr := make(chan error, 1)
	go func() { startErr <- s.Start() }()
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-startErr:
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case <-signalCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Shutdown(shutdownCtx); err != nil {
			fmt.Println("shutdown:", err)
			os.Exit(1)
		}
		if err := <-startErr; err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}
