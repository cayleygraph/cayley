package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Buckets prints a list of all buckets.
func Buckets(path string) {
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
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			println(string(name))
			return nil
		})
	})
	if err != nil {
		fatal(err)
		return
	}
}
