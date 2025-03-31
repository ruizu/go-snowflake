package snowflake

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestNodeID(t *testing.T) {
	for i := 0; i < serverMax; i++ {
		if got, exp := New(i).Node(), i; got != exp {
			t.Fatalf("got %d, expected %d", got, exp)
		}
	}
}

func TestMonotonic(t *testing.T) {
	n := New(10)
	out := make([]int64, 10000)

	for i := range out {
		out[i] = n.ID()
	}

	// ensure they are all distinct and increasing
	for i := range out[1:] {
		if out[i] >= out[i+1] {
			t.Fatal("bad entries:", out[i], out[i+1])
		}
	}
}

func TestCustomEpochMonotonic(t *testing.T) {
	n := NewWithEpoch(10, time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	out := make([]int64, 10000)

	for i := range out {
		out[i] = n.ID()
	}

	// ensure they are all distinct and increasing
	for i := range out[1:] {
		if out[i] >= out[i+1] {
			t.Fatal("bad entries:", out[i], out[i+1])
		}
	}
}

var blackhole int64 // to make sure the n.ID calls are not removed

func BenchmarkID(b *testing.B) {
	n := New(10)

	for i := 0; i < b.N; i++ {
		blackhole += n.ID()
	}
}

func BenchmarkIDParallel(b *testing.B) {
	n := New(1)

	b.RunParallel(func(pb *testing.PB) {
		var lblackhole int64
		for pb.Next() {
			lblackhole += n.ID()
		}
		atomic.AddInt64(&blackhole, lblackhole)
	})
}
