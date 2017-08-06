// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package memstore

import (
	"math"
	"runtime/debug"
	"testing"

	"github.com/cznic/mathutil"
)

func rng() *mathutil.FC32 {
	x, err := mathutil.NewFC32(math.MinInt32/4, math.MaxInt32/4, false)
	if err != nil {
		panic(err)
	}

	return x
}

func BenchmarkSetSeq1e3(b *testing.B) {
	benchmarkSetSeq(b, 1e3)
}

func BenchmarkSetSeq1e4(b *testing.B) {
	benchmarkSetSeq(b, 1e4)
}

func BenchmarkSetSeq1e5(b *testing.B) {
	benchmarkSetSeq(b, 1e5)
}

func BenchmarkSetSeq1e6(b *testing.B) {
	benchmarkSetSeq(b, 1e6)
}

func benchmarkSetSeq(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := TreeNew(cmp)
		debug.FreeOSMemory()
		b.StartTimer()
		for j := int64(0); j < int64(n); j++ {
			r.Set(j, nil)
		}
		b.StopTimer()
		r.Close()
	}
	b.StopTimer()
}

func BenchmarkGetSeq1e3(b *testing.B) {
	benchmarkGetSeq(b, 1e3)
}

func BenchmarkGetSeq1e4(b *testing.B) {
	benchmarkGetSeq(b, 1e4)
}

func BenchmarkGetSeq1e5(b *testing.B) {
	benchmarkGetSeq(b, 1e5)
}

func BenchmarkGetSeq1e6(b *testing.B) {
	benchmarkGetSeq(b, 1e6)
}

func benchmarkGetSeq(b *testing.B, n int) {
	r := TreeNew(cmp)
	for i := int64(0); i < int64(n); i++ {
		r.Set(i, nil)
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := int64(0); j < int64(n); j++ {
			r.Get(j)
		}
	}
	b.StopTimer()
	r.Close()
}

func BenchmarkSetRnd1e3(b *testing.B) {
	benchmarkSetRnd(b, 1e3)
}

func BenchmarkSetRnd1e4(b *testing.B) {
	benchmarkSetRnd(b, 1e4)
}

func BenchmarkSetRnd1e5(b *testing.B) {
	benchmarkSetRnd(b, 1e5)
}

func BenchmarkSetRnd1e6(b *testing.B) {
	benchmarkSetRnd(b, 1e6)
}

func benchmarkSetRnd(b *testing.B, n int) {
	rng := rng()
	a := make([]int, n)
	for i := range a {
		a[i] = rng.Next()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := TreeNew(cmp)
		debug.FreeOSMemory()
		b.StartTimer()
		for _, v := range a {
			r.Set(int64(v), nil)
		}
		b.StopTimer()
		r.Close()
	}
	b.StopTimer()
}

func BenchmarkGetRnd1e3(b *testing.B) {
	benchmarkGetRnd(b, 1e3)
}

func BenchmarkGetRnd1e4(b *testing.B) {
	benchmarkGetRnd(b, 1e4)
}

func BenchmarkGetRnd1e5(b *testing.B) {
	benchmarkGetRnd(b, 1e5)
}

func BenchmarkGetRnd1e6(b *testing.B) {
	benchmarkGetRnd(b, 1e6)
}

func benchmarkGetRnd(b *testing.B, n int) {
	r := TreeNew(cmp)
	rng := rng()
	a := make([]int64, n)
	for i := range a {
		a[i] = int64(rng.Next())
	}
	for _, v := range a {
		r.Set(v, nil)
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			r.Get(v)
		}
	}
	b.StopTimer()
	r.Close()
}

func BenchmarkDelSeq1e3(b *testing.B) {
	benchmarkDelSeq(b, 1e3)
}

func BenchmarkDelSeq1e4(b *testing.B) {
	benchmarkDelSeq(b, 1e4)
}

func BenchmarkDelSeq1e5(b *testing.B) {
	benchmarkDelSeq(b, 1e5)
}

func BenchmarkDelSeq1e6(b *testing.B) {
	benchmarkDelSeq(b, 1e6)
}

func benchmarkDelSeq(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := TreeNew(cmp)
		for j := int64(0); j < int64(n); j++ {
			r.Set(j, nil)
		}
		debug.FreeOSMemory()
		b.StartTimer()
		for j := int64(0); j < int64(n); j++ {
			r.Delete(j)
		}
	}
	b.StopTimer()
}

func BenchmarkDelRnd1e3(b *testing.B) {
	benchmarkDelRnd(b, 1e3)
}

func BenchmarkDelRnd1e4(b *testing.B) {
	benchmarkDelRnd(b, 1e4)
}

func BenchmarkDelRnd1e5(b *testing.B) {
	benchmarkDelRnd(b, 1e5)
}

func BenchmarkDelRnd1e6(b *testing.B) {
	benchmarkDelRnd(b, 1e6)
}

func benchmarkDelRnd(b *testing.B, n int) {
	rng := rng()
	a := make([]int64, n)
	for i := range a {
		a[i] = int64(rng.Next())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := TreeNew(cmp)
		for _, v := range a {
			r.Set(v, nil)
		}
		debug.FreeOSMemory()
		b.StartTimer()
		for _, v := range a {
			r.Delete(v)
		}
		b.StopTimer()
		r.Close()
	}
	b.StopTimer()
}

func BenchmarkSeekSeq1e3(b *testing.B) {
	benchmarkSeekSeq(b, 1e3)
}

func BenchmarkSeekSeq1e4(b *testing.B) {
	benchmarkSeekSeq(b, 1e4)
}

func BenchmarkSeekSeq1e5(b *testing.B) {
	benchmarkSeekSeq(b, 1e5)
}

func BenchmarkSeekSeq1e6(b *testing.B) {
	benchmarkSeekSeq(b, 1e6)
}

func benchmarkSeekSeq(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		t := TreeNew(cmp)
		for j := int64(0); j < int64(n); j++ {
			t.Set(j, nil)
		}
		debug.FreeOSMemory()
		b.StartTimer()
		for j := int64(0); j < int64(n); j++ {
			e, _ := t.Seek(j)
			e.Close()
		}
		b.StopTimer()
		t.Close()
	}
	b.StopTimer()
}

func BenchmarkSeekRnd1e3(b *testing.B) {
	benchmarkSeekRnd(b, 1e3)
}

func BenchmarkSeekRnd1e4(b *testing.B) {
	benchmarkSeekRnd(b, 1e4)
}

func BenchmarkSeekRnd1e5(b *testing.B) {
	benchmarkSeekRnd(b, 1e5)
}

func BenchmarkSeekRnd1e6(b *testing.B) {
	benchmarkSeekRnd(b, 1e6)
}

func benchmarkSeekRnd(b *testing.B, n int) {
	r := TreeNew(cmp)
	rng := rng()
	a := make([]int64, n)
	for i := range a {
		a[i] = int64(rng.Next())
	}
	for _, v := range a {
		r.Set(v, nil)
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			e, _ := r.Seek(v)
			e.Close()
		}
	}
	b.StopTimer()
	r.Close()
}

func BenchmarkNext1e3(b *testing.B) {
	benchmarkNext(b, 1e3)
}

func BenchmarkNext1e4(b *testing.B) {
	benchmarkNext(b, 1e4)
}

func BenchmarkNext1e5(b *testing.B) {
	benchmarkNext(b, 1e5)
}

func BenchmarkNext1e6(b *testing.B) {
	benchmarkNext(b, 1e6)
}

func benchmarkNext(b *testing.B, n int) {
	t := TreeNew(cmp)
	for i := int64(0); i < int64(n); i++ {
		t.Set(i, nil)
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		en, err := t.SeekFirst()
		if err != nil {
			b.Fatal(err)
		}

		m := 0
		for {
			if _, _, err = en.Next(); err != nil {
				break
			}
			m++
		}
		if m != n {
			b.Fatal(m)
		}
	}
	b.StopTimer()
	t.Close()
}

func BenchmarkPrev1e3(b *testing.B) {
	benchmarkPrev(b, 1e3)
}

func BenchmarkPrev1e4(b *testing.B) {
	benchmarkPrev(b, 1e4)
}

func BenchmarkPrev1e5(b *testing.B) {
	benchmarkPrev(b, 1e5)
}

func BenchmarkPrev1e6(b *testing.B) {
	benchmarkPrev(b, 1e6)
}

func benchmarkPrev(b *testing.B, n int) {
	t := TreeNew(cmp)
	for i := int64(0); i < int64(n); i++ {
		t.Set(i, nil)
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		en, err := t.SeekLast()
		if err != nil {
			b.Fatal(err)
		}

		m := 0
		for {
			if _, _, err = en.Prev(); err != nil {
				break
			}
			m++
		}
		if m != n {
			b.Fatal(m)
		}
	}
}
