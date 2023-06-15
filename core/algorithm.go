package core

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	hostReplicaFormat = `%s%d`
)

var (
	defaultReplicaNum = 10
	LoadBoundFactor   = 0.25
	defaultHashFunc   = func(key string) uint64 {
		out := sha512.Sum512([]byte(key))
		return binary.LittleEndian.Uint64(out[:])
	}
)

type Consistent struct {
	replicaNum int
	totalLoad  int64
	hashFunc   func(key string) uint64
	hosts      map[uint64]*Host
	virt2host  map[uint64]string
	ring       []uint64
	sync.RWMutex
}

func 

(replicaNum int, hashFunc func(key string) uint64) *Consistent {
	if replicaNum <= 0 {
		replicaNum = defaultReplicaNum
	}

	if hashFunc == nil {
		hashFunc = defaultHashFunc
	}

	return &Consistent{
		replicaNum:         replicaNum,
		totalLoad:          0,
		hashFunc:           hashFunc,
		hostMap:            make(map[string]*Host),
		replicaHostMap:     make(map[uint64]string),
		sortedHostsHashSet: make([]uint64, 0),
	}
}
func (c *Consistent) RegisterHost(hostName string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.hosts[hostName]; ok {
		return ErrHostAlreadyExists
	}
	c.hosts[hostName] = &Host{
		Name:      hostName,
		LoadBound: 0,
	}

	for i := 0; i < c.replicaNum; i++ {
		hashedIdx := c.hashFunc(fmt.Sprintf(hostReplicaNum, hostName, i))
		c.virt2host[hashedIdx] = hostName
		c.ring = append(c.ring, hashedIdx)
	}
	sort.Slice(c.ring, func(i, j int) bool {
		if c.ring[i] < c.ring[j] {
			return true
		}
		return false
	})
	return nil
}
func (c *Consistent) UnregisterHost(hostName string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.hosts[hostName]; !ok {
		return ErrHostNotFound
	}
	delete(c.hosts, hostName)

	for i := 0; i < c.replicaNum; i++ {
		hashedIdx := c.hashFunc(fmt.Sprintf(hostReplicaFormat, hostName, i))
		c.delHashIndex(hashedIdx)
	}
	return nil
}
func (c *Consistent) UpdateLoad(host string, load int64) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.hosts[host]; !ok {
		return
	}
	c.totalLoad = c.totalLoad - c.hosts[host].loadBound + load
	c.hosts[host].LoadBound = load
}
func (c *Consistent) Hosts() []string {
	c.RLock()
	defer c.RUnlock()

	hosts := make([]string, 0)
	for k := range c.hosts {
		hosts = append(hosts, k)
	}
	return hosts
}
func (c *Consistent) GetHost(key string) (string, error) {
	hashedKey := c.hashFunc(key)
	idx := c.searchKey(hashedKey)
	return c.virt2host[c.ring[idx]], nil
}
func (c *Consistent) GetHostCapacious(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.virt2host) == 0 {
		return "", ErrHostNotFound
	}

	hashedKey := c.hashFunc(key)
	idx := c.searchKey(hashedKey)

	i := idx
	for {
		host := c.virt2host[c.ring[i]]
		loadChecked, err := c.checkLoadCapacity(host)
		if err != nil {
			return "", err
		}
		if loadChecked {
			return host, err
		}
		i++

		if i >= len(c.virt2host) {
			i = 0
		}
	}
}
func (c *Consistent) Inc(hostName string) {
	c.Lock()
	defer c.Unlock()

	atomic.AddInt64(&c.hosts[hostName].LoadBound, 1)
	atomic.AddInt64(&c.totalLoad, 1)
}
func (c *Consistent) Done(host string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.hosts[host]; !ok {
		return
	}
	atomic.AddInt64(&c.hosts[host].LoadBound, -1)
	atomic.AddInt64(&c.totalLoad, -1)
}
func (c *Consistent) GetLoads() map[string]int64 {
	c.RLock()
	defer c.RUnlock()

	loads := make(map[string]int64)
	for k, v := range c.hosts {
		loads[k] = atomic.LoadInt64(&v.LoadBound)
	}
	return loads
}
func (c *Consistent) MaxLoad() int64 {
	if c.totalLoad == 0 {
		c.totalLoad = 1
	}

	var avgLoadPerNode float64
	avgLoadPerNode = float64(c.totalLoad / int64(len(c.hosts)))
	if avgLoadPerNode == 0 {
		avgLoadPerNode = 1
	}
	avgLoadPerNode = math.Ceil(avgLoadPerNode * (1 + loadBoundFactor))
	return int64(avgLoadPerNode)
}

func (c *Consistent) searchKey(key uint64) int {
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] >= key
	})

	if idx >= len(c.ring) {
		// make search as a ring
		idx = 0
	}

	return idx
}
func (c *Consistent) checkLoadCapacity(host string) (bool, error) {

	// a safety check if someone performed c.Done more than needed
	if c.totalLoad < 0 {
		c.totalLoad = 0
	}

	var avgLoadPerNode float64
	avgLoadPerNode = float64((c.totalLoad + 1) / int64(len(c.hosts)))
	if avgLoadPerNode == 0 {
		avgLoadPerNode = 1
	}
	avgLoadPerNode = math.Ceil(avgLoadPerNode * (1 + loadBoundFactor))

	candidateHost, ok := c.hosts[host]
	if !ok {
		return false, ErrHostNotFound
	}

	if float64(candidateHost.LoadBound)+1 <= avgLoadPerNode {
		return true, nil
	}

	return false, nil
}

// 在环中移除某个虚拟服务器的id
func (c *Consistent) delHashIndex(val uint64) {
	idx := -1
	l := 0
	r := len(c.ring) - 1
	for l <= r {
		m := (l + r) / 2
		if c.ring[m] == val {
			idx = m
			break
		} else if c.ring[m] < val {
			l = m + 1
		} else if c.ring[m] > val {
			r = m - 1
		}
	}
	if idx != -1 {
		c.ring = append(c.ring[:idx], c.ring[idx+1:]...)
	}
}
