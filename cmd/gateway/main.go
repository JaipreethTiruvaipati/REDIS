package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/gateway"
)

func main() {
	apiAddr := flag.String("api-addr", envOr("API_ADDR", "127.0.0.1:8080"), "HTTP gateway listen address")
	redisAddr := flag.String("redis-addr", envOr("REDIS_ADDR", "127.0.0.1:6379"), "MyRedis TCP address")
	apiToken := flag.String("api-token", os.Getenv("API_TOKEN"), "optional API bearer token")
	redisUsername := flag.String("redis-username", envOr("REDIS_USERNAME", ""), "Redis username")
	redisPassword := flag.String("redis-password", os.Getenv("REDIS_PASSWORD"), "Redis password")
	flag.Parse()

	g := gateway.New(gateway.Config{APIAddr: *apiAddr, RedisAddr: *redisAddr, APIToken: *apiToken, RedisUsername: *redisUsername, RedisPassword: *redisPassword})
	startErr := make(chan error, 1)
	go func() { startErr <- g.Start() }()
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-startErr:
		if err != nil {
			log.Fatal(err)
		}
	case <-signalCtx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := g.Shutdown(ctx); err != nil {
			log.Printf("gateway shutdown: %v", err)
		}
		if err := <-startErr; err != nil {
			log.Printf("gateway: %v", err)
		}
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
