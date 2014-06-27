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
	"github.com/google/cayley/pkg/graph"
	"sort"
)

func (m *MqlQuery) treeifyResult(tags map[string]graph.TSVal) map[MqlResultPath]string {
	// Transform the map into something a little more interesting.
	results := make(map[MqlPath]string)
	for k, v := range tags {
		results[MqlPath(k)] = m.ses.ts.GetNameFor(v)
	}
	resultPaths := make(map[MqlResultPath]string)
	for k, v := range results {
		resultPaths[k.ToResultPathFromMap(results)] = v
	}

	var paths MqlResultPathSlice

	for path := range resultPaths {
		paths = append(paths, path)
	}

	sort.Sort(paths)

	// Build Structure
	for _, path := range paths {
		currentPath := path.getPath()
		value := resultPaths[path]
		namePath := path.AppendValue(value)
		if _, ok := m.queryResult[namePath]; !ok {
			targetPath, key := path.splitLastPath()
			if path == "" {
				targetPath, key = "", value
				if _, ok := m.queryResult[""][value]; !ok {
					m.resultOrder = append(m.resultOrder, value)
				}
			}
			if _, ok := m.queryStructure[currentPath]; ok {
				// If there's substructure, then copy that in.
				newStruct := m.copyPathStructure(currentPath)
				if m.isRepeated[currentPath] && currentPath != "" {
					switch t := m.queryResult[targetPath][key].(type) {
					case nil:
						x := make([]interface{}, 0)
						x = append(x, newStruct)
						m.queryResult[targetPath][key] = x
						m.queryResult[namePath] = newStruct
					case []interface{}:
						m.queryResult[targetPath][key] = append(t, newStruct)
						m.queryResult[namePath] = newStruct
					}

				} else {
					m.queryResult[namePath] = newStruct
					m.queryResult[targetPath][key] = newStruct
				}
			}
		}
	}

	// Fill values
	for _, path := range paths {
		currentPath := path.getPath()
		value := resultPaths[path]
		namePath := path.AppendValue(value)
		if _, ok := m.queryStructure[currentPath]; ok {
			// We're dealing with ids.
			if _, ok := m.queryResult[namePath]["id"]; ok {
				m.queryResult[namePath]["id"] = value
			}
		} else {
			// Just a value.
			targetPath, key := path.splitLastPath()
			if m.isRepeated[currentPath] {
				switch t := m.queryResult[targetPath][key].(type) {
				case nil:
					x := make([]interface{}, 0)
					x = append(x, value)
					m.queryResult[targetPath][key] = x
				case []interface{}:
					m.queryResult[targetPath][key] = append(t, value)
				}

			} else {
				m.queryResult[targetPath][key] = value
			}
		}
	}

	return resultPaths
}

func (m *MqlQuery) buildResults() {
	for _, v := range m.resultOrder {
		m.results = append(m.results, m.queryResult[""][v])
	}
}
