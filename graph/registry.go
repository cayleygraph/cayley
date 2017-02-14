// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package graph

import (
	"fmt"
)

var (
	ErrQuadStoreNotRegistred = fmt.Errorf("This QuadStore is not registered.")
	ErrOperationNotSupported = fmt.Errorf("This Operation is not supported.")
)

var storeRegistry = make(map[string]QuadStoreRegistration)

type NewStoreFunc func(string, Options) (QuadStore, error)
type InitStoreFunc func(string, Options) error
type UpgradeStoreFunc func(string, Options) error
type NewStoreForRequestFunc func(QuadStore, Options) (QuadStore, error)

type QuadStoreRegistration struct {
	NewFunc           NewStoreFunc
	NewForRequestFunc NewStoreForRequestFunc
	UpgradeFunc       UpgradeStoreFunc
	InitFunc          InitStoreFunc
	IsPersistent      bool
}

func RegisterQuadStore(name string, register QuadStoreRegistration) {
	if register.NewFunc == nil {
		panic("NewFunc must not be nil")
	}

	// Register QuadStore with friendly name
	if _, found := storeRegistry[name]; found {
		panic(fmt.Sprintf("Already registered QuadStore %q.", name))
	}
	storeRegistry[name] = register
}

func NewQuadStore(name string, dbpath string, opts Options) (QuadStore, error) {
	r, registered := storeRegistry[name]
	if !registered {
		return nil, ErrQuadStoreNotRegistred
	}

	return r.NewFunc(dbpath, opts)
}

func NewQuadStoreForRequest(qs QuadStore, opts Options) (QuadStore, error) {
	r, registered := storeRegistry[qs.Type()]
	if !registered {
		return nil, ErrQuadStoreNotRegistred
	}

	if r.NewForRequestFunc == nil {
		return nil, ErrOperationNotSupported
	}

	return r.NewForRequestFunc(qs, opts)
}

func UpgradeQuadStore(name string, dbpath string, opts Options) error {
	r, registered := storeRegistry[name]
	if !registered {
		return ErrQuadStoreNotRegistred
	}

	if r.UpgradeFunc == nil {
		// return ErrOperationNotSupported
		return nil
	}

	return r.UpgradeFunc(dbpath, opts)
}

func InitQuadStore(name string, dbpath string, opts Options) error {
	r, registered := storeRegistry[name]
	if !registered {
		return ErrQuadStoreNotRegistred

	}

	if r.InitFunc == nil {
		return ErrOperationNotSupported
	}

	return r.InitFunc(dbpath, opts)
}

func IsPersistent(name string) bool {
	return storeRegistry[name].IsPersistent
}

func QuadStores() []string {
	t := make([]string, 0, len(storeRegistry))
	for n := range storeRegistry {
		t = append(t, n)
	}
	return t
}
