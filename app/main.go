package main

import (
	"fmt"
	"os"

	"github.com/jaipreethtiruvaipati/redis-clone/app/server"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	s := server.New("0.0.0.0:6379")
	if err := s.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
