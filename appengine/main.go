// +build appenginevm
package main

//go:generate ./sync-assets.sh

import "google.golang.org/appengine"

func main() {
	appengine.Main()
}
