package main

import (
	"log"
	"os"

	"http_server/src/server"
)

func main() {
	log.SetFlags(0)
	if err := server.Run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}
