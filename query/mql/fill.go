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

package mql

import (
	"fmt"
	"sort"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

func (q *Query) treeifyResult(tags map[string]graph.Ref) map[ResultPath]string {
	// Transform the map into something a little more interesting.
	results := make(map[Path]string)
	for k, v := range tags {
		if v == nil {
			continue
		}
		results[Path(k)] = quadValueToNative(q.ses.qs.NameOf(v))
	}
	resultPaths := make(map[ResultPath]string)
	for k, v := range results {
		resultPaths[k.ToResultPathFromMap(results)] = v
	}

	paths := make([]ResultPath, 0, len(resultPaths))
	for path := range resultPaths {
		paths = append(paths, path)
	}
	sort.Sort(byRecordLength(paths))

	// Build Structure
	for _, path := range paths {
		currentPath := path.getPath()
		value := resultPaths[path]
		namePath := path.AppendValue(value)
		if _, ok := q.queryResult[namePath]; !ok {
			targetPath, key := path.splitLastPath()
			if path == "" {
				targetPath, key = "", value
				if _, ok := q.queryResult[""][value]; !ok {
					q.resultOrder = append(q.resultOrder, value)
				}
			}
			if _, ok := q.queryStructure[currentPath]; ok {
				// If there's substructure, then copy that in.
				newStruct := q.copyPathStructure(currentPath)
				if q.isRepeated[currentPath] && currentPath != "" {
					switch t := q.queryResult[targetPath][key].(type) {
					case nil:
						x := make([]interface{}, 0)
						x = append(x, newStruct)
						q.queryResult[targetPath][key] = x
						q.queryResult[namePath] = newStruct
					case []interface{}:
						q.queryResult[targetPath][key] = append(t, newStruct)
						q.queryResult[namePath] = newStruct
					}

				} else {
					q.queryResult[namePath] = newStruct
					q.queryResult[targetPath][key] = newStruct
				}
			}
		}
	}

	// Fill values
	for _, path := range paths {
		currentPath := path.getPath()
		value, ok := resultPaths[path]
		if !ok {
			continue
		}
		namePath := path.AppendValue(value)
		if _, ok := q.queryStructure[currentPath]; ok {
			// We're dealing with ids.
			if _, ok := q.queryResult[namePath]["id"]; ok {
				q.queryResult[namePath]["id"] = value
			}
		} else {
			// Just a value.
			targetPath, key := path.splitLastPath()
			if q.isRepeated[currentPath] {
				switch t := q.queryResult[targetPath][key].(type) {
				case nil:
					x := make([]interface{}, 0)
					x = append(x, value)
					q.queryResult[targetPath][key] = x
				case []interface{}:
					q.queryResult[targetPath][key] = append(t, value)
				}

			} else {
				q.queryResult[targetPath][key] = value
			}
		}
	}

	return resultPaths
}

func (q *Query) buildResults() {
	for _, v := range q.resultOrder {
		q.results = append(q.results, q.queryResult[""][v])
	}
}

func quadValueToNative(v quad.Value) string {
	out := quad.NativeOf(v)
	if nv, ok := out.(quad.Value); ok && v == nv {
		return quad.StringOf(v)
	}
	return fmt.Sprint(out)
}
