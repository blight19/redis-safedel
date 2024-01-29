package main

import (
	"github.com/blight19/redis-safedel/api"
	"github.com/blight19/redis-safedel/service"
	"net"
	"net/http"
)

func main() {
	go func() {
		mux := api.GetHttpMux()
		err := http.ListenAndServe(":8081", mux)
		if err != nil {
			panic(err)
		}
	}()
	listener, err := net.Listen("tcp", ":3333")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		go func() {
			name := "test"
			service.HandleConnection(conn, name)
		}()
	}
}
