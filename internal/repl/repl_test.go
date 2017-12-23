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

package repl

import (
	"testing"
)

var testSplitLines = []struct {
	line              string
	expectedCommand   string
	expectedArguments string
	err               error
}{
	{
		line:              ":a arg1 arg2 arg3 .",
		expectedCommand:   ":a",
		expectedArguments: " arg1 arg2 arg3 .",
	},
	{
		line:              ":debug t",
		expectedCommand:   ":debug",
		expectedArguments: " t",
	},
	{
		line: "",
		// expectedCommand is nil
		// expectedArguments is nil
	},
	{
		line:              `:d <http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> . # comments here`,
		expectedCommand:   ":d",
		expectedArguments: ` <http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> . # comments here`,
	},
	{
		line:              `  :a  subject  "predicate with spaces" object  . `,
		expectedCommand:   ":a",
		expectedArguments: `  subject  "predicate with spaces" object  .`,
	},
}

func TestSplitLines(t *testing.T) {
	for _, testcase := range testSplitLines {
		command, arguments := splitLine(testcase.line)

		if testcase.expectedCommand != command {
			t.Errorf("Error splitting lines: got: %v expected: %v", command, testcase.expectedCommand)
		}

		if testcase.expectedArguments != arguments {
			t.Errorf("Error splitting lines: got: %v expected: %v", arguments, testcase.expectedArguments)
		}
	}
}
