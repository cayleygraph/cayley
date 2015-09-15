package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
)

// Ensure that a database info can be printed.
func TestInfo(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			b.Put([]byte("foo"), []byte("0000"))
			return nil
		})
		db.Close()
		output := run("info", path)
		equals(t, `Page Size: 4096`, output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestInfo_NotFound(t *testing.T) {
	SetTestMode(true)
	output := run("info", "no/such/db")
	equals(t, "stat no/such/db: no such file or directory", output)
}
