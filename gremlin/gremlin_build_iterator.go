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
		return graph.NewNullIterator()
	}
	return buildIteratorTreeHelper(obj, ts, graph.NewNullIterator())
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
		return ts.GetNodesAllIterator()
	}
	if val.IsPrimitive() {
		thing, _ := val.Export()
		switch v := thing.(type) {
		case string:
			it := ts.MakeFixed()
			it.AddValue(ts.GetIdFor(v))
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
		it := ts.MakeFixed()
		for _, x := range strings {
			it.AddValue(ts.GetIdFor(x))
		}
		return it
	case "Number":
		fallthrough
	case "Boolean":
		fallthrough
	case "Date":
		fallthrough
	case "String":
		it := ts.MakeFixed()
		str, _ := val.ToString()
		it.AddValue(ts.GetIdFor(str))
		return it
	default:
		glog.Errorln("Trying to handle unsupported Javascript value.")
		return graph.NewNullIterator()
	}
}

func buildInOutIterator(obj *otto.Object, ts graph.TripleStore, base graph.Iterator, isReverse bool) graph.Iterator {
	argList, _ := obj.Get("_gremlin_values")
	if argList.Class() != "GoArray" {
		glog.Errorln("How is arglist not an array? Return nothing.", argList.Class())
		return graph.NewNullIterator()
	}
	argArray := argList.Object()
	lengthVal, _ := argArray.Get("length")
	length, _ := lengthVal.ToInteger()
	var predicateNodeIterator graph.Iterator
	if length == 0 {
		predicateNodeIterator = ts.GetNodesAllIterator()
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

	in, out := "s", "o"
	if isReverse {
		in, out = out, in
	}
	lto := graph.NewLinksToIterator(ts, base, in)
	and := graph.NewAndIterator()
	and.AddSubIterator(graph.NewLinksToIterator(ts, predicateNodeIterator, "p"))
	and.AddSubIterator(lto)
	return graph.NewHasaIterator(ts, and, out)
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
			it = ts.GetNodesAllIterator()
		} else {
			fixed := ts.MakeFixed()
			for _, name := range stringArgs {
				fixed.AddValue(ts.GetIdFor(name))
			}
			it = fixed
		}
	case "tag":
		it = subIt
		for _, tag := range stringArgs {
			it.AddTag(tag)
		}
	case "save":
		all := ts.GetNodesAllIterator()
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return graph.NewNullIterator()
		}
		if len(stringArgs) == 2 {
			all.AddTag(stringArgs[1])
		} else {
			all.AddTag(stringArgs[0])
		}
		predFixed := ts.MakeFixed()
		predFixed.AddValue(ts.GetIdFor(stringArgs[0]))
		subAnd := graph.NewAndIterator()
		subAnd.AddSubIterator(graph.NewLinksToIterator(ts, predFixed, "p"))
		subAnd.AddSubIterator(graph.NewLinksToIterator(ts, all, "o"))
		hasa := graph.NewHasaIterator(ts, subAnd, "s")
		and := graph.NewAndIterator()
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "saver":
		all := ts.GetNodesAllIterator()
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return graph.NewNullIterator()
		}
		if len(stringArgs) == 2 {
			all.AddTag(stringArgs[1])
		} else {
			all.AddTag(stringArgs[0])
		}
		predFixed := ts.MakeFixed()
		predFixed.AddValue(ts.GetIdFor(stringArgs[0]))
		subAnd := graph.NewAndIterator()
		subAnd.AddSubIterator(graph.NewLinksToIterator(ts, predFixed, "p"))
		subAnd.AddSubIterator(graph.NewLinksToIterator(ts, all, "s"))
		hasa := graph.NewHasaIterator(ts, subAnd, "o")
		and := graph.NewAndIterator()
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "has":
		fixed := ts.MakeFixed()
		if len(stringArgs) < 2 {
			return graph.NewNullIterator()
		}
		for _, name := range stringArgs[1:] {
			fixed.AddValue(ts.GetIdFor(name))
		}
		predFixed := ts.MakeFixed()
		predFixed.AddValue(ts.GetIdFor(stringArgs[0]))
		subAnd := graph.NewAndIterator()
		subAnd.AddSubIterator(graph.NewLinksToIterator(ts, predFixed, "p"))
		subAnd.AddSubIterator(graph.NewLinksToIterator(ts, fixed, "o"))
		hasa := graph.NewHasaIterator(ts, subAnd, "s")
		and := graph.NewAndIterator()
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "morphism":
		it = base
	case "and":
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if !isVertexChain(firstArg.Object()) {
			return graph.NewNullIterator()
		}
		argIt := buildIteratorTree(firstArg.Object(), ts)

		and := graph.NewAndIterator()
		and.AddSubIterator(subIt)
		and.AddSubIterator(argIt)
		it = and
	case "back":
		arg, _ := obj.Get("_gremlin_back_chain")
		argIt := buildIteratorTree(arg.Object(), ts)
		and := graph.NewAndIterator()
		and.AddSubIterator(subIt)
		and.AddSubIterator(argIt)
		it = and
	case "is":
		fixed := ts.MakeFixed()
		for _, name := range stringArgs {
			fixed.AddValue(ts.GetIdFor(name))
		}
		and := graph.NewAndIterator()
		and.AddSubIterator(fixed)
		and.AddSubIterator(subIt)
		it = and
	case "or":
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if !isVertexChain(firstArg.Object()) {
			return graph.NewNullIterator()
		}
		argIt := buildIteratorTree(firstArg.Object(), ts)

		or := graph.NewOrIterator()
		or.AddSubIterator(subIt)
		or.AddSubIterator(argIt)
		it = or
	case "both":
		// Hardly the most efficient pattern, but the most general.
		// Worth looking into an Optimize() optimization here.
		clone := subIt.Clone()
		it1 := buildInOutIterator(obj, ts, subIt, false)
		it2 := buildInOutIterator(obj, ts, clone, true)

		or := graph.NewOrIterator()
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
			return graph.NewNullIterator()
		}
		it = buildIteratorTreeHelper(firstArg.Object(), ts, subIt)
	case "followr":
		// Follow a morphism
		arg, _ := obj.Get("_gremlin_followr")
		if isVertexChain(arg.Object()) {
			return graph.NewNullIterator()
		}
		it = buildIteratorTreeHelper(arg.Object(), ts, subIt)
	case "in":
		it = buildInOutIterator(obj, ts, subIt, true)
	}
	return it
}
