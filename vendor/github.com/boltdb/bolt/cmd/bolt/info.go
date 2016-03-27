package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Info prints basic information about a database.
func Info(path string) {
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

	// Print basic database info.
	var info = db.Info()
	printf("Page Size: %d\n", info.PageSize)
}
