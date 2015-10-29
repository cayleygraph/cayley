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
	"fmt"
	"strconv"

	"github.com/barakmich/glog"
	"github.com/robertkrimen/otto"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/graph/path"
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
	path := buildPathFromObject(obj)
	if path == nil {
		return iterator.NewNull()
	}
	return path.BuildIteratorOn(qs)
}

func getFirstArgAsVertexChain(obj *otto.Object) *otto.Object {
	arg, _ := obj.Get("_gremlin_values")
	firstArg, _ := arg.Object().Get("0")
	if !isVertexChain(firstArg.Object()) {
		return nil
	}
	return firstArg.Object()
}

func getFirstArgAsMorphismChain(obj *otto.Object) *otto.Object {
	arg, _ := obj.Get("_gremlin_values")
	firstArg, _ := arg.Object().Get("0")
	if isVertexChain(firstArg.Object()) {
		return nil
	}
	return firstArg.Object()
}

func buildPathFromObject(obj *otto.Object) *path.Path {
	var p *path.Path
	val, _ := obj.Get("_gremlin_type")
	stringArgs := propertiesOf(obj, "string_args")
	gremlinType := val.String()
	if prev, _ := obj.Get("_gremlin_prev"); !prev.IsObject() {
		switch gremlinType {
		case "vertex":
			return path.StartMorphism(stringArgs...)
		case "morphism":
			return path.StartMorphism()
		default:
			panic("No base gremlin path other than 'vertex' or 'morphism'")
		}
	} else {
		p = buildPathFromObject(prev.Object())
	}
	if p == nil {
		return nil
	}
	switch gremlinType {
	case "Is":
		return p.Is(stringArgs...)
	case "In":
		preds, tags, ok := getViaData(obj)
		if !ok {
			return nil
		}
		return p.InWithTags(tags, preds...)
	case "Out":
		preds, tags, ok := getViaData(obj)
		if !ok {
			return nil
		}
		return p.OutWithTags(tags, preds...)
	case "Both":
		preds, _, ok := getViaData(obj)
		if !ok {
			return nil
		}
		return p.Both(preds...)
	case "Follow":
		subobj := getFirstArgAsMorphismChain(obj)
		if subobj == nil {
			return nil
		}
		return p.Follow(buildPathFromObject(subobj))
	case "FollowR":
		subobj := getFirstArgAsMorphismChain(obj)
		if subobj == nil {
			return nil
		}
		return p.FollowReverse(buildPathFromObject(subobj))
	case "And", "Intersect":
		subobj := getFirstArgAsVertexChain(obj)
		if subobj == nil {
			return nil
		}
		return p.And(buildPathFromObject(subobj))
	case "Union", "Or":
		subobj := getFirstArgAsVertexChain(obj)
		if subobj == nil {
			return nil
		}
		return p.Or(buildPathFromObject(subobj))
	case "Back":
		if len(stringArgs) != 1 {
			return nil
		}
		return p.Back(stringArgs[0])
	case "Tag", "As":
		return p.Tag(stringArgs...)
	case "Has":
		if len(stringArgs) < 2 {
			return nil
		}
		return p.Has(stringArgs[0], stringArgs[1:]...)
	case "Save", "SaveR":
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return nil
		}
		tag := stringArgs[0]
		if len(stringArgs) == 2 {
			tag = stringArgs[1]
		}
		if gremlinType == "SaveR" {
			return p.SaveReverse(stringArgs[0], tag)
		}
		return p.Save(stringArgs[0], tag)
	case "Except", "Difference":
		subobj := getFirstArgAsVertexChain(obj)
		if subobj == nil {
			return nil
		}
		return p.Except(buildPathFromObject(subobj))
	case "InPredicates":
		return p.InPredicates()
	case "OutPredicates":
		return p.OutPredicates()
	default:
		panic(fmt.Sprint("Unimplemented Gremlin function", gremlinType))
	}
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

func buildPathFromValue(val otto.Value) (out []interface{}) {
	if val.IsNull() || val.IsUndefined() {
		return nil
	}
	if val.IsPrimitive() {
		thing, _ := val.Export()
		switch v := thing.(type) {
		case string:
			out = append(out, v)
			return
		default:
			glog.Errorln("Trying to build unknown primitive value.")
		}
	}
	switch val.Class() {
	case "Object":
		out = append(out, buildPathFromObject(val.Object()))
		return
	case "Array":
		// Had better be an array of strings
		for _, x := range stringsFrom(val.Object()) {
			out = append(out, x)
		}
		return
	case "Number":
		fallthrough
	case "Boolean":
		fallthrough
	case "Date":
		fallthrough
	case "String":
		out = append(out, val.String())
		return
	default:
		glog.Errorln("Trying to handle unsupported Javascript value.")
		return nil
	}
}

func getViaData(obj *otto.Object) (predicates []interface{}, tags []string, ok bool) {
	argList, _ := obj.Get("_gremlin_values")
	if argList.Class() != "GoArray" {
		glog.Errorln("How is arglist not an array? Return nothing.", argList.Class())
		return nil, nil, false
	}
	argArray := argList.Object()
	lengthVal, _ := argArray.Get("length")
	length, _ := lengthVal.ToInteger()
	if length == 0 {
		predicates = []interface{}{}
	} else {
		zero, _ := argArray.Get("0")
		predicates = buildPathFromValue(zero)
	}
	if length >= 2 {
		one, _ := argArray.Get("1")
		if one.IsString() {
			tags = append(tags, one.String())
		} else if one.Class() == "Array" {
			tags = stringsFrom(one.Object())
		}
	}
	ok = true
	return
}
