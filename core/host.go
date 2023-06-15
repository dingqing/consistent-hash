package core

type Host struct {
	// host id: ip:port
	Name string
	// 服务器容量限制
	LoadBound int64
}
