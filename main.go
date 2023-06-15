package main

import (
	"fmt"
	"github.com/dingqing/consistent-hash/core"
	"github.com/dingqing/consistent-hash/proxy"
	"net/http"
)

var (
	port = "18888"

	p = proxy.New(core.New(10, nil))
)

func main() {
	stopChan := make(chan interface{})
	start(port)
	<-stopChan
}

func start(port string) {
	http.HandleFunc("/register", registerHost)
	http.HandleFunc("/unregister", unregisterHost)
	http.HandleFunc("/host", GetHost)
	http.HandleFunc("/hostCapacious", GetHostCapacious)

	fmt.Printf("start proxy server: %s\n", port)

	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		panic(err)
	}
}

func registerHost(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	err := p.RegisterHost(r.Form["host"][0])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, err.Error())
		return
	}

	_, _ = fmt.Fprintf(w, fmt.Sprintf("register host: %s success", r.Form["host"][0]))
}

func unregisterHost(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	err := p.UnregisterHost(r.Form["host"][0])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, err.Error())
		return
	}

	_, _ = fmt.Fprintf(w, fmt.Sprintf("unregister host: %s success", r.Form["host"][0]))
}

func GetHost(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	val, err := p.GetHost(r.Form["key"][0])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, err.Error())
		return
	}

	_, _ = fmt.Fprintf(w, fmt.Sprintf("key: %s, val: %s", r.Form["key"][0], val))
}

func GetHostCapacious(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	val, err := p.GetHostCapacious(r.Form["key"][0])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, err.Error())
		return
	}

	_, _ = fmt.Fprintf(w, fmt.Sprintf("key: %s, val: %s", r.Form["key"][0], val))
}
