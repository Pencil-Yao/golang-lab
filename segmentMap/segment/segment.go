package segment

import (
	"encoding/binary"
	"sync"
)

const (
	u64c = 12200160415121876738
)

type SegMaps []SegmentMap

type SegmentMap struct {
	data		map[string]interface{}
	sync.RWMutex
}

func (s SegmentMap) getValue(key string) (interface{}, bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s SegmentMap) setValue(key string, v interface{}) {
	s.Lock()
	defer s.Unlock()
	s.data[key] = v
}

func hashUint64(m string) (hash uint64) {
	for i := 0; i < len(m) / 8; i++ {
		n := u64c ^ binary.BigEndian.Uint64([]byte(m[i*8:i*8+7]))
		hash ^= n
	}
	return
}

func (maps SegMaps) findSegmentU64(m string) SegmentMap {
	return maps[hashUint64(m) % uint64(len(maps))]
}

func (maps SegMaps) Get(key string) (interface{}, bool) {
	s := maps.findSegmentU64(key)
	return s.getValue(key)
}

func (maps SegMaps) Set(key string, v interface{}) {
	s := maps.findSegmentU64(key)
	s.setValue(key, v)
}

func (maps SegMaps) GetAllKeys() (sts []string) {
	ch := make(chan string, len(maps))
	wg := sync.WaitGroup{}
	wg.Add(len(maps))
	for _, s :=range maps {
		go func(s SegmentMap) {
			s.RLock()
			for km := range s.data {
				ch <- km
			}
			s.RUnlock()
			wg.Done()
		}(s)
	}
	wg.Wait()
	close(ch)

	for s := range ch {
		sts = append(sts, s)
	}
	return
}

func CreateSegMaps(num int) SegMaps {
	maps := make(SegMaps, num)
	for n, _ := range maps {
		maps[n].data = make(map[string]interface{})
	}
	return maps
}
