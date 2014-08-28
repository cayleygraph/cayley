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

package gremlin

import (
	"strconv"

	"github.com/barakmich/glog"
	"github.com/robertkrimen/otto"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func propertiesOf(obj *otto.Object, name string) []string {
	val, _ := obj.Get(name)
	if val.IsUndefined() {
		return nil
	}
	export, _ := val.Export()
	return export.([]string)
}

func buildIteratorTree(obj *otto.Object, qs graph.QuadStore) graph.Iterator {
	if !isVertexChain(obj) {
		return iterator.NewNull()
	}
	return buildIteratorTreeHelper(obj, qs, iterator.NewNull())
}

func stringsFrom(obj *otto.Object) []string {
	var output []string
	lengthValue, _ := obj.Get("length")
	length, _ := lengthValue.ToInteger()
	ulength := uint32(length)
	for i := uint32(0); i < ulength; i++ {
		name := strconv.FormatInt(int64(i), 10)
		value, err := obj.Get(name)
		if err != nil || !value.IsString() {
			continue
		}
		output = append(output, value.String())
	}
	return output
}

func buildIteratorFromValue(val otto.Value, qs graph.QuadStore) graph.Iterator {
	if val.IsNull() || val.IsUndefined() {
		return qs.NodesAllIterator()
	}
	if val.IsPrimitive() {
		thing, _ := val.Export()
		switch v := thing.(type) {
		case string:
			it := qs.FixedIterator()
			it.Add(qs.ValueOf(v))
			return it
		default:
			glog.Errorln("Trying to build unknown primitive value.")
		}
	}
	switch val.Class() {
	case "Object":
		return buildIteratorTree(val.Object(), qs)
	case "Array":
		// Had better be an array of strings
		strings := stringsFrom(val.Object())
		it := qs.FixedIterator()
		for _, x := range strings {
			it.Add(qs.ValueOf(x))
		}
		return it
	case "Number":
		fallthrough
	case "Boolean":
		fallthrough
	case "Date":
		fallthrough
	case "String":
		it := qs.FixedIterator()
		it.Add(qs.ValueOf(val.String()))
		return it
	default:
		glog.Errorln("Trying to handle unsupported Javascript value.")
		return iterator.NewNull()
	}
}

func buildInOutIterator(obj *otto.Object, qs graph.QuadStore, base graph.Iterator, isReverse bool) graph.Iterator {
	argList, _ := obj.Get("_gremlin_values")
	if argList.Class() != "GoArray" {
		glog.Errorln("How is arglist not an array? Return nothing.", argList.Class())
		return iterator.NewNull()
	}
	argArray := argList.Object()
	lengthVal, _ := argArray.Get("length")
	length, _ := lengthVal.ToInteger()
	var predicateNodeIterator graph.Iterator
	if length == 0 {
		predicateNodeIterator = qs.NodesAllIterator()
	} else {
		zero, _ := argArray.Get("0")
		predicateNodeIterator = buildIteratorFromValue(zero, qs)
	}
	if length >= 2 {
		var tags []string
		one, _ := argArray.Get("1")
		if one.IsString() {
			tags = append(tags, one.String())
		} else if one.Class() == "Array" {
			tags = stringsFrom(one.Object())
		}
		for _, tag := range tags {
			predicateNodeIterator.Tagger().Add(tag)
		}
	}

	in, out := quad.Subject, quad.Object
	if isReverse {
		in, out = out, in
	}
	lto := iterator.NewLinksTo(qs, base, in)
	and := iterator.NewAnd()
	and.AddSubIterator(iterator.NewLinksTo(qs, predicateNodeIterator, quad.Predicate))
	and.AddSubIterator(lto)
	return iterator.NewHasA(qs, and, out)
}

func buildIteratorTreeHelper(obj *otto.Object, qs graph.QuadStore, base graph.Iterator) graph.Iterator {
	it := base

	// TODO: Better error handling
	var subIt graph.Iterator
	if prev, _ := obj.Get("_gremlin_prev"); !prev.IsObject() {
		subIt = base
	} else {
		subIt = buildIteratorTreeHelper(prev.Object(), qs, base)
	}

	stringArgs := propertiesOf(obj, "string_args")
	val, _ := obj.Get("_gremlin_type")
	switch val.String() {
	case "vertex":
		if len(stringArgs) == 0 {
			it = qs.NodesAllIterator()
		} else {
			fixed := qs.FixedIterator()
			for _, name := range stringArgs {
				fixed.Add(qs.ValueOf(name))
			}
			it = fixed
		}
	case "tag":
		it = subIt
		for _, tag := range stringArgs {
			it.Tagger().Add(tag)
		}
	case "save":
		all := qs.NodesAllIterator()
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return iterator.NewNull()
		}
		if len(stringArgs) == 2 {
			all.Tagger().Add(stringArgs[1])
		} else {
			all.Tagger().Add(stringArgs[0])
		}
		predFixed := qs.FixedIterator()
		predFixed.Add(qs.ValueOf(stringArgs[0]))
		subAnd := iterator.NewAnd()
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, predFixed, quad.Predicate))
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, all, quad.Object))
		hasa := iterator.NewHasA(qs, subAnd, quad.Subject)
		and := iterator.NewAnd()
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "saver":
		all := qs.NodesAllIterator()
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return iterator.NewNull()
		}
		if len(stringArgs) == 2 {
			all.Tagger().Add(stringArgs[1])
		} else {
			all.Tagger().Add(stringArgs[0])
		}
		predFixed := qs.FixedIterator()
		predFixed.Add(qs.ValueOf(stringArgs[0]))
		subAnd := iterator.NewAnd()
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, predFixed, quad.Predicate))
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, all, quad.Subject))
		hasa := iterator.NewHasA(qs, subAnd, quad.Object)
		and := iterator.NewAnd()
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "has":
		fixed := qs.FixedIterator()
		if len(stringArgs) < 2 {
			return iterator.NewNull()
		}
		for _, name := range stringArgs[1:] {
			fixed.Add(qs.ValueOf(name))
		}
		predFixed := qs.FixedIterator()
		predFixed.Add(qs.ValueOf(stringArgs[0]))
		subAnd := iterator.NewAnd()
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, predFixed, quad.Predicate))
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, fixed, quad.Object))
		hasa := iterator.NewHasA(qs, subAnd, quad.Subject)
		and := iterator.NewAnd()
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "morphism":
		it = base
	case "and":
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if !isVertexChain(firstArg.Object()) {
			return iterator.NewNull()
		}
		argIt := buildIteratorTree(firstArg.Object(), qs)

		and := iterator.NewAnd()
		and.AddSubIterator(subIt)
		and.AddSubIterator(argIt)
		it = and
	case "back":
		arg, _ := obj.Get("_gremlin_back_chain")
		argIt := buildIteratorTree(arg.Object(), qs)
		and := iterator.NewAnd()
		and.AddSubIterator(subIt)
		and.AddSubIterator(argIt)
		it = and
	case "is":
		fixed := qs.FixedIterator()
		for _, name := range stringArgs {
			fixed.Add(qs.ValueOf(name))
		}
		and := iterator.NewAnd()
		and.AddSubIterator(fixed)
		and.AddSubIterator(subIt)
		it = and
	case "or":
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if !isVertexChain(firstArg.Object()) {
			return iterator.NewNull()
		}
		argIt := buildIteratorTree(firstArg.Object(), qs)

		or := iterator.NewOr()
		or.AddSubIterator(subIt)
		or.AddSubIterator(argIt)
		it = or
	case "both":
		// Hardly the most efficient pattern, but the most general.
		// Worth looking into an Optimize() optimization here.
		clone := subIt.Clone()
		it1 := buildInOutIterator(obj, qs, subIt, false)
		it2 := buildInOutIterator(obj, qs, clone, true)

		or := iterator.NewOr()
		or.AddSubIterator(it1)
		or.AddSubIterator(it2)
		it = or
	case "out":
		it = buildInOutIterator(obj, qs, subIt, false)
	case "follow":
		// Follow a morphism
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if isVertexChain(firstArg.Object()) {
			return iterator.NewNull()
		}
		it = buildIteratorTreeHelper(firstArg.Object(), qs, subIt)
	case "followr":
		// Follow a morphism
		arg, _ := obj.Get("_gremlin_followr")
		if isVertexChain(arg.Object()) {
			return iterator.NewNull()
		}
		it = buildIteratorTreeHelper(arg.Object(), qs, subIt)
	case "in":
		it = buildInOutIterator(obj, qs, subIt, true)
	}
	return it
}
