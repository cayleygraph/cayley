package main_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
)

func TestStats(t *testing.T) {
	if os.Getpagesize() != 4096 {
		t.Skip()
	}
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("foo"))
			if err != nil {
				return err
			}
			for i := 0; i < 10; i++ {
				b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}
			b, err = tx.CreateBucket([]byte("bar"))
			if err != nil {
				return err
			}
			for i := 0; i < 100; i++ {
				b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}
			b, err = tx.CreateBucket([]byte("baz"))
			if err != nil {
				return err
			}
			b.Put([]byte("key"), []byte("value"))
			return nil
		})
		db.Close()
		output := run("stats", path, "b")
		equals(t, "Aggregate statistics for 2 buckets\n\n"+
			"Page count statistics\n"+
			"\tNumber of logical branch pages: 0\n"+
			"\tNumber of physical branch overflow pages: 0\n"+
			"\tNumber of logical leaf pages: 1\n"+
			"\tNumber of physical leaf overflow pages: 0\n"+
			"Tree statistics\n"+
			"\tNumber of keys/value pairs: 101\n"+
			"\tNumber of levels in B+tree: 1\n"+
			"Page size utilization\n"+
			"\tBytes allocated for physical branch pages: 0\n"+
			"\tBytes actually used for branch data: 0 (0%)\n"+
			"\tBytes allocated for physical leaf pages: 4096\n"+
			"\tBytes actually used for leaf data: 1996 (48%)\n"+
			"Bucket statistics\n"+
			"\tTotal number of buckets: 2\n"+
			"\tTotal number on inlined buckets: 1 (50%)\n"+
			"\tBytes used for inlined buckets: 40 (2%)", output)
	})
}
