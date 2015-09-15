package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Get retrieves the value for a given bucket/key.
func Get(path, name, key string) {
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

		// Find value for a given key.
		value := b.Get([]byte(key))
		if value == nil {
			fatalf("key not found: %s", key)
			return nil
		}

		println(string(value))
		return nil
	})
	if err != nil {
		fatal(err)
		return
	}
}
