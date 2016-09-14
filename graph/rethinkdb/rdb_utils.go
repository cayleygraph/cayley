package rethinkdb

import (
	"errors"
	"fmt"
	"time"

	"gopkg.in/dancannon/gorethink.v2"
)

// openSession initializes the rethink db session
func openSession(opts gorethink.ConnectOpts, maxWait time.Duration) (session *gorethink.Session, err error) {
	gorethink.SetTags("gorethink", "json")

	addr := opts.Address
	if addr == "" && len(opts.Addresses) == 0 {
		err = errors.New("Missing address")
		return
	}
	if addr == "" {
		addr = opts.Addresses[0]
	}

	if maxWait > 0 {
		done := time.Now().Add(maxWait)
		for time.Now().Before(done) {
			session, err = gorethink.Connect(opts)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			err = fmt.Errorf("Failed to connect to %s for %v: %v", addr, maxWait, err)
			return
		}
	} else {
		session, err = gorethink.Connect(opts)
		if err != nil {
			err = fmt.Errorf("Failed to connect to %s: %v", addr, err)
			return
		}
	}

	// Create database if not already exists
	exists, err := databaseExists(opts.Database, session)
	if err != nil {
		session.Close()
		return
	}

	if !exists {
		if err = gorethink.DBCreate(opts.Database).Exec(session); err != nil {
			session.Close()
			return
		}
	}

	session.Use(opts.Database)
	return
}

// databaseExists checks if a database exists
func databaseExists(name string, s *gorethink.Session) (exists bool, err error) {
	res, err := gorethink.DBList().Run(s)
	if err != nil {
		return
	}
	defer res.Close()
	var dbs []string
	if err = res.All(&dbs); err != nil {
		return
	}

	for _, t := range dbs {
		if t == name {
			exists = true
			break
		}
	}
	return
}

// tableExists checks if a table exists
func tableExists(name string, s *gorethink.Session) (exists bool, err error) {
	res, err := gorethink.TableList().Run(s)
	if err != nil {
		return
	}
	defer res.Close()
	var tables []string
	if err = res.All(&tables); err != nil {
		return
	}
	for _, t := range tables {
		if t == name {
			exists = true
			return
		}
	}
	return
}

// ensureTable creates a table if it does not already exist
func ensureTable(name string, s *gorethink.Session) error {
	if exists, err := tableExists(name, s); err != nil || exists {
		return err
	}

	return gorethink.TableCreate(name).Exec(s)
}

// indexExists checks if a table index exists
func indexExists(table gorethink.Term, index string, s *gorethink.Session) (exists bool, err error) {
	res, err := table.IndexList().Run(s)
	if err != nil {
		return
	}
	defer res.Close()
	var indices []string
	if err = res.All(&indices); err != nil {
		return
	}
	for _, t := range indices {
		if t == index {
			exists = true
			return
		}
	}
	return
}

// ensureIndex creates an index if not already exist
func ensureIndex(table gorethink.Term, index string, s *gorethink.Session) error {
	if exists, err := indexExists(table, index, s); err != nil || exists {
		return err
	}

	return table.IndexCreate(index).Exec(s)
}

// ensureIndexFunc creates an index if not already exist
func ensureIndexFunc(table gorethink.Term, index string, indexFunction interface{}, s *gorethink.Session) error {
	if exists, err := indexExists(table, index, s); err != nil || exists {
		return err
	}

	return table.IndexCreateFunc(index, indexFunction).Exec(s)
}

// sliceBatch splits a slice into batches of size
func sliceBatch(slice []interface{}, size int) (b [][]interface{}) {
	lena := len(slice)
	lenb := lena/size + 1
	b = make([][]interface{}, lenb)

	for i := range b {
		start := i * size
		end := start + size
		if end > lena {
			end = lena
		}
		b[i] = slice[start:end]
	}
	return
}
