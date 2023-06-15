package proxy

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/dingqing/consistent-hash/core"
)

type Proxy struct {
	consistent *core.Consistent
}

func New(consistent *core.Consistent) *Proxy {
	proxy := &Proxy{
		consistent: consistent,
	}
	return proxy
}

func (p *Proxy) GetHost(key string) (string, error) {

	host, err := p.consistent.GetHost(key)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(fmt.Sprintf("http://%s?key=%s", host, key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	fmt.Printf("Response from host %s: %s\n", host, string(body))

	return string(body), nil
}

func (p *Proxy) GetHostCapacious(key string) (string, error) {

	host, err := p.consistent.GetHostCapacious(key)
	if err != nil {
		return "", err
	}
	p.consistent.Inc(host)

	time.AfterFunc(time.Second*10, func() { // drop the host after 10 seconds(for testing)!
		fmt.Printf("dropping host: %s after 10 second\n", host)
		p.consistent.Done(host)
	})

	resp, err := http.Get(fmt.Sprintf("http://%s?key=%s", host, key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	fmt.Printf("Response from host %s: %s\n", host, string(body))

	return string(body), nil
}

func (p *Proxy) RegisterHost(host string) error {

	err := p.consistent.RegisterHost(host)
	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("register host: %s success", host))
	return nil
}

func (p *Proxy) UnregisterHost(host string) error {
	err := p.consistent.UnregisterHost(host)
	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("unregister host: %s success", host))
	return nil
}
