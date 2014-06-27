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

// Defines the graph session interface general to all query languages.

type ParseResult int

const (
	Parsed ParseResult = iota
	ParseMore
	ParseFail
)

type Session interface {
	// Return whether the string is a valid expression.
	InputParses(string) (ParseResult, error)
	ExecInput(string, chan interface{}, int)
	ToText(interface{}) string
	ToggleDebug()
}

type HttpSession interface {
	// Return whether the string is a valid expression.
	InputParses(string) (ParseResult, error)
	// Runs the query and returns individual results on the channel.
	ExecInput(string, chan interface{}, int)
	GetQuery(string, chan map[string]interface{})
	BuildJson(interface{})
	GetJson() (interface{}, error)
	ClearJson()
	ToggleDebug()
}
