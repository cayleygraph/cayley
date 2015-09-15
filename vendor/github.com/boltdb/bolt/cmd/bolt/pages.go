package main

import (
	"os"
	"strconv"

	"github.com/boltdb/bolt"
)

// Pages prints a list of all pages in a database.
func Pages(path string) {
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

	println("ID       TYPE       ITEMS  OVRFLW")
	println("======== ========== ====== ======")

	db.Update(func(tx *bolt.Tx) error {
		var id int
		for {
			p, err := tx.Page(id)
			if err != nil {
				fatalf("page error: %d: %s", id, err)
			} else if p == nil {
				break
			}

			// Only display count and overflow if this is a non-free page.
			var count, overflow string
			if p.Type != "free" {
				count = strconv.Itoa(p.Count)
				if p.OverflowCount > 0 {
					overflow = strconv.Itoa(p.OverflowCount)
				}
			}

			// Print table row.
			printf("%-8d %-10s %-6s %-6s\n", p.ID, p.Type, count, overflow)

			// Move to the next non-overflow page.
			id += 1
			if p.Type != "free" {
				id += p.OverflowCount
			}
		}
		return nil
	})
}
