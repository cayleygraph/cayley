package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Keys retrieves a list of keys for a given bucket.
func Keys(path, name string) {
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
		// Find bucket.
		b := tx.Bucket([]byte(name))
		if b == nil {
			fatalf("bucket not found: %s", name)
			return nil
		}

		// Iterate over each key.
		return b.ForEach(func(key, _ []byte) error {
			println(string(key))
			return nil
		})
	})
	if err != nil {
		fatal(err)
		return
	}
}
