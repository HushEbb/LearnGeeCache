package consistenthash

import (
	"hash/crc32"
	"slices"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash         Hash
	replicas     int
	virtualNodes []int // Sorted
	hashMap      map[int]string
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		hash:         fn,
		replicas:     replicas,
		virtualNodes: make([]int, 0),
		hashMap:      make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// add node/machine to hash space
func (m *Map) Add(nodes ...string) {
	for _, node := range nodes {
		for i := range m.replicas {
			hash := int(m.hash([]byte(strconv.Itoa(i) + node)))
			m.virtualNodes = append(m.virtualNodes, hash)
			m.hashMap[hash] = node
		}
	}
	slices.Sort(m.virtualNodes)
}

func (m *Map) Get(key string) string {
	if len(m.virtualNodes) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	idx, _ := slices.BinarySearch(m.virtualNodes, hash)
	return m.hashMap[m.virtualNodes[idx%len(m.virtualNodes)]]
}
