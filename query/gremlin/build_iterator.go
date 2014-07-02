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
)

func getStrings(obj *otto.Object, field string) []string {
	strings := make([]string, 0)
	val, _ := obj.Get(field)
	if !val.IsUndefined() {
		export, _ := val.Export()
		array := export.([]interface{})
		for _, arg := range array {
			strings = append(strings, arg.(string))
		}
	}
	return strings
}

func getStringArgs(obj *otto.Object) []string { return getStrings(obj, "string_args") }

func buildIteratorTree(obj *otto.Object, ts graph.TripleStore) graph.Iterator {
	if !isVertexChain(obj) {
		return iterator.NewNull()
	}
	return buildIteratorTreeHelper(obj, ts, iterator.NewNull())
}

func makeListOfStringsFromArrayValue(obj *otto.Object) []string {
	var output []string
	lengthValue, _ := obj.Get("length")
	length, _ := lengthValue.ToInteger()
	ulength := uint32(length)
	for index := uint32(0); index < ulength; index += 1 {
		name := strconv.FormatInt(int64(index), 10)
		value, err := obj.Get(name)
		if err != nil {
			continue
		}
		if !value.IsString() {
			continue
		}
		s, _ := value.ToString()
		output = append(output, s)
	}
	return output
}

func buildIteratorFromValue(val otto.Value, ts graph.TripleStore) graph.Iterator {
	if val.IsNull() || val.IsUndefined() {
		return ts.NodesAllIterator()
	}
	if val.IsPrimitive() {
		thing, _ := val.Export()
		switch v := thing.(type) {
		case string:
			it := ts.FixedIterator()
			it.AddValue(ts.ValueOf(v))
			return it
		default:
			glog.Errorln("Trying to build unknown primitive value.")
		}
	}
	switch val.Class() {
	case "Object":
		return buildIteratorTree(val.Object(), ts)
	case "Array":
		// Had better be an array of strings
		strings := makeListOfStringsFromArrayValue(val.Object())
		it := ts.FixedIterator()
		for _, x := range strings {
			it.AddValue(ts.ValueOf(x))
		}
		return it
	case "Number":
		fallthrough
	case "Boolean":
		fallthrough
	case "Date":
		fallthrough
	case "String":
		it := ts.FixedIterator()
		str, _ := val.ToString()
		it.AddValue(ts.ValueOf(str))
		return it
	default:
		glog.Errorln("Trying to handle unsupported Javascript value.")
		return iterator.NewNull()
	}
}

func buildInOutIterator(obj *otto.Object, ts graph.TripleStore, base graph.Iterator, isReverse bool) graph.Iterator {
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
		predicateNodeIterator = ts.NodesAllIterator()
	} else {
		zero, _ := argArray.Get("0")
		predicateNodeIterator = buildIteratorFromValue(zero, ts)
	}
	if length >= 2 {
		var tags []string
		one, _ := argArray.Get("1")
		if one.IsString() {
			s, _ := one.ToString()
			tags = append(tags, s)
		} else if one.Class() == "Array" {
			tags = makeListOfStringsFromArrayValue(one.Object())
		}
		for _, tag := range tags {
			predicateNodeIterator.AddTag(tag)
		}
	}

	in, out := graph.Subject, graph.Object
	if isReverse {
		in, out = out, in
	}
	lto := iterator.NewLinksTo(ts, base, in)
	and := iterator.NewAnd()
	and.AddSubIterator(iterator.NewLinksTo(ts, predicateNodeIterator, graph.Predicate))
	and.AddSubIterator(lto)
	return iterator.NewHasA(ts, and, out)
}

func buildIteratorTreeHelper(obj *otto.Object, ts graph.TripleStore, base graph.Iterator) graph.Iterator {
	var it graph.Iterator
	it = base
	// TODO: Better error handling
	kindVal, _ := obj.Get("_gremlin_type")
	stringArgs := getStringArgs(obj)
	var subIt graph.Iterator
	prevVal, _ := obj.Get("_gremlin_prev")
	if !prevVal.IsObject() {
		subIt = base
	} else {
		subIt = buildIteratorTreeHelper(prevVal.Object(), ts, base)
	}

	kind, _ := kindVal.ToString()
	switch kind {
	case "vertex":
		if len(stringArgs) == 0 {
			it = ts.NodesAllIterator()
		} else {
			fixed := ts.FixedIterator()
			for _, name := range stringArgs {
				fixed.AddValue(ts.ValueOf(name))
			}
			it = fixed
		}
	case "tag":
		it = subIt
		for _, tag := range stringArgs {
			it.AddTag(tag)
		}
	case "save":
		all := ts.NodesAllIterator()
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return iterator.NewNull()
		}
		if len(stringArgs) == 2 {
			all.AddTag(stringArgs[1])
		} else {
			all.AddTag(stringArgs[0])
		}
		predFixed := ts.FixedIterator()
		predFixed.AddValue(ts.ValueOf(stringArgs[0]))
		subAnd := iterator.NewAnd()
		subAnd.AddSubIterator(iterator.NewLinksTo(ts, predFixed, graph.Predicate))
		subAnd.AddSubIterator(iterator.NewLinksTo(ts, all, graph.Object))
		hasa := iterator.NewHasA(ts, subAnd, graph.Subject)
		and := iterator.NewAnd()
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "saver":
		all := ts.NodesAllIterator()
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return iterator.NewNull()
		}
		if len(stringArgs) == 2 {
			all.AddTag(stringArgs[1])
		} else {
			all.AddTag(stringArgs[0])
		}
		predFixed := ts.FixedIterator()
		predFixed.AddValue(ts.ValueOf(stringArgs[0]))
		subAnd := iterator.NewAnd()
		subAnd.AddSubIterator(iterator.NewLinksTo(ts, predFixed, graph.Predicate))
		subAnd.AddSubIterator(iterator.NewLinksTo(ts, all, graph.Subject))
		hasa := iterator.NewHasA(ts, subAnd, graph.Object)
		and := iterator.NewAnd()
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "has":
		fixed := ts.FixedIterator()
		if len(stringArgs) < 2 {
			return iterator.NewNull()
		}
		for _, name := range stringArgs[1:] {
			fixed.AddValue(ts.ValueOf(name))
		}
		predFixed := ts.FixedIterator()
		predFixed.AddValue(ts.ValueOf(stringArgs[0]))
		subAnd := iterator.NewAnd()
		subAnd.AddSubIterator(iterator.NewLinksTo(ts, predFixed, graph.Predicate))
		subAnd.AddSubIterator(iterator.NewLinksTo(ts, fixed, graph.Object))
		hasa := iterator.NewHasA(ts, subAnd, graph.Subject)
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
		argIt := buildIteratorTree(firstArg.Object(), ts)

		and := iterator.NewAnd()
		and.AddSubIterator(subIt)
		and.AddSubIterator(argIt)
		it = and
	case "back":
		arg, _ := obj.Get("_gremlin_back_chain")
		argIt := buildIteratorTree(arg.Object(), ts)
		and := iterator.NewAnd()
		and.AddSubIterator(subIt)
		and.AddSubIterator(argIt)
		it = and
	case "is":
		fixed := ts.FixedIterator()
		for _, name := range stringArgs {
			fixed.AddValue(ts.ValueOf(name))
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
		argIt := buildIteratorTree(firstArg.Object(), ts)

		or := iterator.NewOr()
		or.AddSubIterator(subIt)
		or.AddSubIterator(argIt)
		it = or
	case "both":
		// Hardly the most efficient pattern, but the most general.
		// Worth looking into an Optimize() optimization here.
		clone := subIt.Clone()
		it1 := buildInOutIterator(obj, ts, subIt, false)
		it2 := buildInOutIterator(obj, ts, clone, true)

		or := iterator.NewOr()
		or.AddSubIterator(it1)
		or.AddSubIterator(it2)
		it = or
	case "out":
		it = buildInOutIterator(obj, ts, subIt, false)
	case "follow":
		// Follow a morphism
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if isVertexChain(firstArg.Object()) {
			return iterator.NewNull()
		}
		it = buildIteratorTreeHelper(firstArg.Object(), ts, subIt)
	case "followr":
		// Follow a morphism
		arg, _ := obj.Get("_gremlin_followr")
		if isVertexChain(arg.Object()) {
			return iterator.NewNull()
		}
		it = buildIteratorTreeHelper(arg.Object(), ts, subIt)
	case "in":
		it = buildInOutIterator(obj, ts, subIt, true)
	}
	return it
}
