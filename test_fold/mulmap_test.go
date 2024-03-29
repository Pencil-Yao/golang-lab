package test_fold

import (
	"github.com/Pencil-Yao/golang-lab/LockMap"
	"github.com/Pencil-Yao/golang-lab/segmentMap/segment"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

var SHARD_COUNT = 32
var r = rand.New(rand.NewSource(123456789))

// 插入不存在的 key (粗糙的锁)
func BenchmarkSingleInsertAbsentBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		myMap.BuiltinMapStore(strconv.Itoa(i), "value")
	}
}

// 插入不存在的 key (分段锁)
func BenchmarkSingleInsertAbsent(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(strconv.Itoa(i), "value")
	}
}

// 插入不存在的 key (syncMap)
func BenchmarkSingleInsertAbsentSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		syncMap.Store(strconv.Itoa(i), "value")
	}
}

// 插入存在 key (粗糙锁)
func BenchmarkSingleInsertPresentBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	myMap.BuiltinMapStore("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		myMap.BuiltinMapStore("key", "value")
	}
}

// 插入存在 key (分段锁)
func BenchmarkSingleInsertPresent(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set("key", "value")
	}
}

// 插入存在 key (syncMap)
func BenchmarkSingleInsertPresentSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	syncMap.Store("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		syncMap.Store("key", "value")
	}
}

// 读取存在 key (粗糙锁)
func BenchmarkSingleGetPresentBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	myMap.BuiltinMapStore("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		myMap.BuiltinMapLookup("key")
	}
}

// 读取存在 key (分段锁)
func BenchmarkSingleGetPresent(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get("key")
	}
}

// 读取存在 key (syncMap)
func BenchmarkSingleGetPresentSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	syncMap.Store("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		syncMap.Load("key")
	}
}

// 删除存在 key (粗糙锁)
func BenchmarkDeleteBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().Unix()))
		for pb.Next() {
			// The loop body is executed b.N times total across all goroutines.
			k := r.Intn(100000000)
			myMap.BuiltinMapDelete(strconv.Itoa(k))
		}
	})
}

// 删除存在 key (分段锁)
func BenchmarkDelete(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().Unix()))
		for pb.Next() {
			// The loop body is executed b.N times total across all goroutines.
			k := r.Intn(100000000)
			m.Remove(strconv.Itoa(k))
		}
	})
}

// 删除存在 key (syncMap)
func BenchmarkDeleteSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().Unix()))
		for pb.Next() {
			// The loop body is executed b.N times total across all goroutines.
			k := r.Intn(100000000)
			syncMap.Delete(strconv.Itoa(k))
		}
	})
}

// 并发的插入不存在的 key-value (粗糙锁)
func BenchmarkMultiInsertDifferentBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	finished := make(chan struct{}, b.N)

	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapStore(key, value)
		}
		finished <- struct{}{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set(strconv.Itoa(i), "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

// 并发的插入不存在的 key-value (分段锁)
func benchmarkMultiInsertDifferent(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	finished := make(chan struct{}, b.N)
	_, set := GetSet(m, finished)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set(strconv.Itoa(i), "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiInsertDifferent_1_Shard(b *testing.B) {
	runWithShards(benchmarkMultiInsertDifferent, b, 1)
}
func BenchmarkMultiInsertDifferent_16_Shard(b *testing.B) {
	runWithShards(benchmarkMultiInsertDifferent, b, 16)
}
func BenchmarkMultiInsertDifferent_32_Shard(b *testing.B) {
	runWithShards(benchmarkMultiInsertDifferent, b, 32)
}
func BenchmarkMultiInsertDifferent_256_Shard(b *testing.B) {
	runWithShards(benchmarkMultiInsertDifferent, b, 256)
}

// 并发的插入不存在的 key-value (syncMap)
func BenchmarkMultiInsertDifferentSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	finished := make(chan struct{}, b.N)

	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			syncMap.Store(key, value)
		}
		finished <- struct{}{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set(strconv.Itoa(i), "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

// 并发的插入相同的 key-value (粗糙锁)
func BenchmarkMultiInsertSameBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	finished := make(chan struct{}, b.N)

	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapStore(key, value)
		}
		finished <- struct{}{}
	}
	myMap.BuiltinMapStore("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set("key", "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

// 并发的插入相同的 key-value (分段锁)
func BenchmarkMultiInsertSame(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	finished := make(chan struct{}, b.N)
	_, set := GetSet(m, finished)
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set("key", "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

// 并发的插入相同的 key-value (syncMap)
func BenchmarkMultiInsertSameSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	finished := make(chan struct{}, b.N)

	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			syncMap.Store(key, value)
		}
		finished <- struct{}{}
	}
	syncMap.Store("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set("key", "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

// 并发的 get (粗糙锁)
func BenchmarkMultiGetSameBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	finished := make(chan struct{}, b.N)
	get := func(key, value string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapLookup(key)
		}
		finished <- struct{}{}
	}
	myMap.BuiltinMapStore("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go get("key", "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

// 并发的 get (分段锁)
func BenchmarkMultiGetSame(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	finished := make(chan struct{}, b.N)
	get, _ := GetSet(m, finished)
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go get("key", "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

// 并发的 get (syncMap)
func BenchmarkMultiGetSameSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	finished := make(chan struct{}, b.N)
	get := func(key, value string) {
		for i := 0; i < 10; i++ {
			syncMap.Load(key)
		}
		finished <- struct{}{}
	}
	syncMap.Store("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go get("key", "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

// 并发的 get 和 set (粗糙锁)
func BenchmarkMultiGetSetDifferentBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	finished := make(chan struct{}, 2*b.N)
	get := func(key, value string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapLookup(key)
		}
		finished <- struct{}{}
	}
	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapStore(key, value)
		}
		finished <- struct{}{}
	}
	myMap.BuiltinMapStore("-1", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i-1), "value")
		go get(strconv.Itoa(i), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

// 并发的 get 和 set（分段锁）
func benchmarkMultiGetSetDifferent(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet(m, finished)
	m.Set("-1", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i-1), "value")
		go get(strconv.Itoa(i), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiGetSetDifferent_1_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetDifferent, b, 1)
}
func BenchmarkMultiGetSetDifferent_16_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetDifferent, b, 16)
}
func BenchmarkMultiGetSetDifferent_32_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetDifferent, b, 32)
}
func BenchmarkMultiGetSetDifferent_256_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetDifferent, b, 256)
}

// 并发的 get 和 set (syncMap)
func BenchmarkMultiGetSetDifferentSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	finished := make(chan struct{}, 2*b.N)
	get := func(key, value string) {
		for i := 0; i < 10; i++ {
			syncMap.Load(key)
		}
		finished <- struct{}{}
	}
	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			syncMap.Store(key, value)
		}
		finished <- struct{}{}
	}
	syncMap.Store("-1", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i-1), "value")
		go get(strconv.Itoa(i), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

// set new key, get exit key (粗糙锁)
func BenchmarkMultiGetSetBlockBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	finished := make(chan struct{}, 2*b.N)
	get := func(key, value string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapLookup(key)
		}
		finished <- struct{}{}
	}
	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapStore(key, value)
		}
		finished <- struct{}{}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i%10000), "value")
		go get(strconv.Itoa(i%10000), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

// set new key, get exit key（分段锁）
func benchmarkMultiGetSetBlock(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet(m, finished)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i%10000), "value")
		go get(strconv.Itoa(i%10000), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiGetSetBlock_1_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetBlock, b, 1)
}
func BenchmarkMultiGetSetBlock_16_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetBlock, b, 16)
}
func BenchmarkMultiGetSetBlock_32_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetBlock, b, 32)
}
func BenchmarkMultiGetSetBlock_256_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetBlock, b, 256)
}

// set new key, get exit key (syncMap)
func BenchmarkMultiGetSetBlockSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	finished := make(chan struct{}, 2*b.N)
	get := func(key, value string) {
		for i := 0; i < 10; i++ {
			syncMap.Load(key)
		}
		finished <- struct{}{}
	}
	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			syncMap.Store(key, value)
		}
		finished <- struct{}{}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i%10000), "value")
		go get(strconv.Itoa(i%10000), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

// get set random key (粗糙锁)
func BenchmarkMultiGetSetRandomBuiltInMap(b *testing.B) {
	myMap := LockMap.New()
	finished := make(chan struct{}, 2*b.N)
	get := func(key, _ string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapLookup(key + strconv.Itoa(i))
		}
		finished <- struct{}{}
	}
	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			myMap.BuiltinMapStore(key+strconv.Itoa(i), value)
		}
		finished <- struct{}{}
	}

	myMap.BuiltinMapStore("-1", "value")
	b.ResetTimer()

	for idx := 0; idx < b.N; idx++ {
		go set(strconv.Itoa(r.Intn(b.N)), "value"+strconv.Itoa(r.Intn(b.N)))
		go get(strconv.Itoa(r.Intn(b.N)), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

// get set random key（分段锁）
func benchmarkMultiGetSetRandom(b *testing.B) {
	m := segment.CreateSegMaps(SHARD_COUNT)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet2(m, b.N, finished)
	m.Set("-1", "value")
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		go set(strconv.Itoa(r.Intn(b.N)), "value"+strconv.Itoa(r.Intn(b.N)))
		go get(strconv.Itoa(r.Intn(b.N)), "value")
	}

	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiGetSetRandom_1_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetRandom, b, 1)
}
func BenchmarkMultiGetSetRandom_16_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetRandom, b, 16)
}
func BenchmarkMultiGetSetRandom_32_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetRandom, b, 32)
}
func BenchmarkMultiGetSetRandom_256_Shard(b *testing.B) {
	runWithShards(benchmarkMultiGetSetRandom, b, 256)
}

// get set random key (syncMap)
func BenchmarkMultiGetSetRandomSyncMap(b *testing.B) {
	syncMap := &sync.Map{}
	finished := make(chan struct{}, 2*b.N)
	get := func(key, _ string) {
		for i := 0; i < 10; i++ {
			syncMap.Load(key + strconv.Itoa(i))
		}
		finished <- struct{}{}
	}
	set := func(key, value string) {
		for i := 0; i < 10; i++ {
			syncMap.Store(key+strconv.Itoa(i), value)
		}
		finished <- struct{}{}
	}

	syncMap.Store("-1", "value")
	b.ResetTimer()

	for idx := 0; idx < b.N; idx++ {
		go set(strconv.Itoa(r.Intn(b.N)), "value"+strconv.Itoa(r.Intn(b.N)))
		go get(strconv.Itoa(r.Intn(b.N)), "value")
	}

	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func GetSet(m segment.SegMaps, finished chan struct{}) (set func(key, value string), get func(key, value string)) {
	return func(key, value string) {
			for i := 0; i < 10; i++ {
				m.Get(key)
			}
			finished <- struct{}{}
		}, func(key, value string) {
			for i := 0; i < 10; i++ {
				m.Set(key, value)
			}
			finished <- struct{}{}
		}
}

func GetSet2(m segment.SegMaps, n int, finished chan struct{}) (set func(key, value string), get func(key, value string)) {
	return func(key, value string) {
			for i := 0; i < 10; i++ {
				m.Get(key + strconv.Itoa(i))
			}

			finished <- struct{}{}
		}, func(key, value string) {
			for i := 0; i < 10; i++ {
				m.Set(key+strconv.Itoa(i), value)
			}

			finished <- struct{}{}
		}
}

func runWithShards(bench func(b *testing.B), b *testing.B, shardsCount int) {
	oldShardsCount := SHARD_COUNT
	SHARD_COUNT = shardsCount
	bench(b)
	SHARD_COUNT = oldShardsCount
}
