package rethinkdb

import "gopkg.in/dancannon/gorethink.v2"

// openSession initializes the rethink db session
func openSession(address string, database string) (session *gorethink.Session, err error) {
	gorethink.SetTags("gorethink", "json")

	session, err = gorethink.Connect(gorethink.ConnectOpts{
		Address:  address,
		Database: database,
	})
	if err != nil {
		return
	}

	// Create database if not already exists
	exists, err := databaseExists(database, session)
	if err != nil {
		session.Close()
		return
	}

	if !exists {
		if err = gorethink.DBCreate(database).Exec(session); err != nil {
			session.Close()
			return
		}
	}

	session.Use(database)

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
