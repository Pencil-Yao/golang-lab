package LockMap

import "sync"

type LockMap struct {
	sync.Mutex
	m map[string]interface{}
}

func New() *LockMap {
	return &LockMap{m: make(map[string]interface{}, 32)}
}

func (m *LockMap) BuiltinMapStore(k string, v interface{}) {
	m.Lock()
	defer m.Unlock()
	m.m[k] = v
}

func (m *LockMap) BuiltinMapLookup(k string) interface{} {
	m.Lock()
	defer m.Unlock()
	if v, ok := m.m[k]; !ok {
		return -1
	} else {
		return v
	}
}

func (m *LockMap) BuiltinMapDelete(k string) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.m[k]; !ok {
		return
	} else {
		delete(m.m, k)
	}
}