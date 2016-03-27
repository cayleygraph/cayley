package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Check performs a consistency check on the database and prints any errors found.
func Check(path string) {
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

	// Perform consistency check.
	_ = db.View(func(tx *bolt.Tx) error {
		var count int
		ch := tx.Check()
	loop:
		for {
			select {
			case err, ok := <-ch:
				if !ok {
					break loop
				}
				println(err)
				count++
			}
		}

		// Print summary of errors.
		if count > 0 {
			fatalf("%d errors found", count)
		} else {
			println("OK")
		}
		return nil
	})
}
