package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Server struct {
	KvMap sync.Map
	Lock  sync.RWMutex
}

var (
	server = Server{KvMap: sync.Map{}}

	port = flag.String("p", "8080", "port")

	regHost = "http://localhost:18888"

	expireTime = 10
)

func main() {
	flag.Parse()

	stopChan := make(chan interface{})
	start(*port)
	<-stopChan
}

func start(port string) {
	hostName := fmt.Sprintf("localhost:%s", port)

	fmt.Printf("start server: %s\n", port)

	err := registerHost(hostName)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/", kvHandle)
	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		err = unregisterHost(hostName)
		if err != nil {
			panic(err)
		}
		panic(err)
	}
}

func kvHandle(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	if _, ok := server.KvMap.Load(r.Form["key"][0]); !ok {
		val := fmt.Sprintf("hello: %s", r.Form["key"][0])
		server.KvMap.Store(r.Form["key"][0], val)
		fmt.Printf("cached key: {%s: %s}\n", r.Form["key"][0], val)

		time.AfterFunc(time.Duration(expireTime)*time.Second, func() {
			server.KvMap.Delete(r.Form["key"][0])
			fmt.Printf("removed cached key after 10s: {%s: %s}\n", r.Form["key"][0], val)
		})
	}

	val, _ := server.KvMap.Load(r.Form["key"][0])

	_, err := fmt.Fprintf(w, val.(string))
	if err != nil {
		panic(err)
	}
}

func registerHost(host string) error {
	resp, err := http.Get(fmt.Sprintf("%s/register?host=%s", regHost, host))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func unregisterHost(host string) error {
	resp, err := http.Get(fmt.Sprintf("%s/unregister?host=%s", regHost, host))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
