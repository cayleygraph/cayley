// +build !js

package ouch

import (
	_ "github.com/go-kivik/couchdb" // The CouchDB driver
)

var defaultDriverName = "couch"
