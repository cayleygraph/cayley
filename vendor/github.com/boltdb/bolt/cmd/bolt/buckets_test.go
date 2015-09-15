package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
)

// Ensure that a list of buckets can be retrieved.
func TestBuckets(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("woojits"))
			tx.CreateBucket([]byte("widgets"))
			tx.CreateBucket([]byte("whatchits"))
			return nil
		})
		db.Close()
		output := run("buckets", path)
		equals(t, "whatchits\nwidgets\nwoojits", output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestBucketsDBNotFound(t *testing.T) {
	SetTestMode(true)
	output := run("buckets", "no/such/db")
	equals(t, "stat no/such/db: no such file or directory", output)
}
