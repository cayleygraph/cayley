package rethinkdb

import (
	"time"

	"gopkg.in/dancannon/gorethink.v2"
)

// openSession initializes the rethink db session
func openSession(address string, database string) (c *gorethink.Session, err error) {
	gorethink.SetTags("gorethink", "json")

	for i := 0; i < 10; i++ {
		c, err = gorethink.Connect(gorethink.ConnectOpts{
			Address:  address,
			Database: database,
			MaxIdle:  10,
			MaxOpen:  10,
		})
		if err == nil {
			break
		} else {
			time.Sleep(time.Second * 2)
		}
	}

	if err != nil {
		return
	}

	// Create database if not already exists
	exists, err := databaseExists(database, c)
	if err != nil {
		return
	}

	if !exists {
		err = gorethink.DBCreate(database).Exec(c)
		if err != nil {
			return
		}
	}

	c.Use(database)

	return
}

// databaseExists checks if a database exists
func databaseExists(name string, s *gorethink.Session) (exists bool, err error) {
	exists = false
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
	exists = false
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
func ensureTable(name string, s *gorethink.Session) (err error) {
	var exists bool
	if exists, err = tableExists(name, s); err != nil || exists {
		return
	}

	err = gorethink.TableCreate(name).Exec(s)
	return
}

// indexExists checks if a table index exists
func indexExists(table gorethink.Term, index string, s *gorethink.Session) (exists bool, err error) {
	exists = false
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
func ensureIndex(table gorethink.Term, index string, s *gorethink.Session) (err error) {
	var exists bool
	if exists, err = indexExists(table, index, s); err != nil || exists {
		return
	}

	err = table.IndexCreate(index).Exec(s)
	return
}

// ensureIndexFunc creates an index if not already exist
func ensureIndexFunc(table gorethink.Term, index string, indexFunction interface{}, s *gorethink.Session) (err error) {
	var exists bool
	if exists, err = indexExists(table, index, s); err != nil || exists {
		return
	}

	err = table.IndexCreateFunc(index, indexFunction).Exec(s)
	return
}
