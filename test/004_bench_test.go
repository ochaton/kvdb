package main_test

import (
	"sync/atomic"
	"testing"

	"github.com/ochaton/kvdb"
	"github.com/ochaton/kvdb/test/helpers"
)

func BenchmarkWrite(b *testing.B) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		b.Fatalf("%v", err)
	}

	sp := kvdb.GetSpace[helpers.Metric](db, "users")
	time := int64(0)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(p *testing.PB) {
		var m helpers.Metric
		k := []byte("metric")
		for p.Next() {
			time++
			m.Time = time
			m.Value = time
			m.Name = k
			if err := sp.Set(m.Key(), m); err != nil {
				b.Fatalf("failed to set metric: %v", err)
			}
		}
	})

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		b.Fatalf("%v", err)
	}
}

func BenchmarkTXRandomKeys(b *testing.B) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		b.Fatalf("%v", err)
	}
	time := atomic.Int64{}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(p *testing.PB) {
		var m helpers.Metric
		k := []byte("metric")
		for p.Next() {
			time.Add(1)
			m.Time = time.Load()
			m.Value = m.Time
			m.Name = k
			err := db.Tx(func(exec kvdb.Executer) error {
				sp := kvdb.GetSpace[helpers.Metric](exec, "metrics")
				if err := sp.Set(m.Key(), m); err != nil {
					return err
				}
				return nil
			})

			if err != nil {
				b.Fatalf("failed to set metric: %v", err)
			}
		}
	})

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		b.Fatalf("%v", err)
	}
}

func BenchmarkRandomKeys(b *testing.B) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		b.Fatalf("%v", err)
	}
	sp := kvdb.GetSpace[helpers.Metric](db, "metrics")

	time := atomic.Int64{}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(p *testing.PB) {
		var m helpers.Metric
		k := []byte("metric")
		for p.Next() {
			time.Add(1)
			m.Time = time.Load()
			m.Value = m.Time
			m.Name = k

			if err := sp.Set(m.Key(), m); err != nil {
				b.Fatalf("failed to set metric: %v", err)
			}
		}
	})

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		b.Fatalf("%v", err)
	}
}
