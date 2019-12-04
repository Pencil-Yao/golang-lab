package segment

import (
	"github.com/Pencil-Yao/golang-lab/segmentMap/segment"
	"strconv"
	"sync"
	"testing"
)

func BenchmarkSegmentMapSet(b *testing.B) {
	maps := segment.CreateSegMaps(32)
	var (
		wg sync.WaitGroup
		begain = make(chan struct{})
	)

	benchFunc := func() {
		<- begain
		for idx := 0; idx < 100000; idx++ {
			maps.Set(strconv.Itoa(idx), idx)
		}
		wg.Done()
	}
	wg.Add(1)
	go benchFunc()
	b.StartTimer()
	close(begain)
	wg.Wait()
}
