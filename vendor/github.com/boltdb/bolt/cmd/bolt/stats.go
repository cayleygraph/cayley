package main

import (
	"bytes"
	"os"

	"github.com/boltdb/bolt"
)

// Collect stats for all top level buckets matching the prefix.
func Stats(path, prefix string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fatal(err)
		return
	}

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		var s bolt.BucketStats
		var count int
		var prefix = []byte(prefix)
		tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if bytes.HasPrefix(name, prefix) {
				s.Add(b.Stats())
				count += 1
			}
			return nil
		})
		printf("Aggregate statistics for %d buckets\n\n", count)

		println("Page count statistics")
		printf("\tNumber of logical branch pages: %d\n", s.BranchPageN)
		printf("\tNumber of physical branch overflow pages: %d\n", s.BranchOverflowN)
		printf("\tNumber of logical leaf pages: %d\n", s.LeafPageN)
		printf("\tNumber of physical leaf overflow pages: %d\n", s.LeafOverflowN)

		println("Tree statistics")
		printf("\tNumber of keys/value pairs: %d\n", s.KeyN)
		printf("\tNumber of levels in B+tree: %d\n", s.Depth)

		println("Page size utilization")
		printf("\tBytes allocated for physical branch pages: %d\n", s.BranchAlloc)
		var percentage int
		if s.BranchAlloc != 0 {
			percentage = int(float32(s.BranchInuse) * 100.0 / float32(s.BranchAlloc))
		}
		printf("\tBytes actually used for branch data: %d (%d%%)\n", s.BranchInuse, percentage)
		printf("\tBytes allocated for physical leaf pages: %d\n", s.LeafAlloc)
		percentage = 0
		if s.LeafAlloc != 0 {
			percentage = int(float32(s.LeafInuse) * 100.0 / float32(s.LeafAlloc))
		}
		printf("\tBytes actually used for leaf data: %d (%d%%)\n", s.LeafInuse, percentage)

		println("Bucket statistics")
		printf("\tTotal number of buckets: %d\n", s.BucketN)
		percentage = int(float32(s.InlineBucketN) * 100.0 / float32(s.BucketN))
		printf("\tTotal number on inlined buckets: %d (%d%%)\n", s.InlineBucketN, percentage)
		percentage = 0
		if s.LeafInuse != 0 {
			percentage = int(float32(s.InlineBucketInuse) * 100.0 / float32(s.LeafInuse))
		}
		printf("\tBytes used for inlined buckets: %d (%d%%)\n", s.InlineBucketInuse, percentage)

		return nil
	})
	if err != nil {
		fatal(err)
		return
	}
}
