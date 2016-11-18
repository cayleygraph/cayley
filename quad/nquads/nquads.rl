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

// Ragel gramar definition derived from http://www.w3.org/TR/n-quads/#sec-grammar.

%%{
	machine nquads;

	alphtype rune;

	PN_CHARS_BASE           = [A-Za-z]
							| 0x00c0 .. 0x00d6
							| 0x00d8 .. 0x00f6
							| 0x00f8 .. 0x02ff
							| 0x0370 .. 0x037d
							| 0x037f .. 0x1fff
							| 0x200c .. 0x200d
							| 0x2070 .. 0x218f
							| 0x2c00 .. 0x2fef
							| 0x3001 .. 0xd7ff
							| 0xf900 .. 0xfdcf
							| 0xfdf0 .. 0xfffd
							| 0x10000 .. 0xeffff
							;

	PN_CHARS_U              = PN_CHARS_BASE | '_' | ':' ;

	PN_CHARS                = PN_CHARS_U
							| '-'
							| [0-9]
							| 0xb7
							| 0x0300 .. 0x036f
							| 0x203f .. 0x2040
							;

	ECHAR                   = ('\\' [tbnrf"'\\]) %Escape ;

	UCHAR                   = ('\\u' xdigit {4}
							| '\\U' xdigit {8}) %Escape
							;

	BLANK_NODE_LABEL        = '_:' (PN_CHARS_U | [0-9]) ((PN_CHARS | '.')* PN_CHARS)? ;

	STRING_LITERAL          = (
							  '!'
							| '#' .. '['
							| ']' .. 0x7e
							| 0x80 .. 0x10ffff
							| ECHAR
							| UCHAR)+ - ('_:' | any* '.' | '#' any*)
							;

	STRING_LITERAL_QUOTE    = '"' (
							  0x00 .. 0x09
							| 0x0b .. 0x0c
							| 0x0e .. '!'
							| '#' .. '['
							| ']' .. 0x10ffff
							| ECHAR
							| UCHAR)*
							  '"'
							;

	IRIREF                  = '<' (
							  '!'
							| '#' .. ';'
							| '='
							| '?' .. '['
							| ']'
							| '_'
							| 'a' .. 'z'
							| '~'
							| 0x80 .. 0x10ffff
							| UCHAR)*
							  '>'
							;

	LANGTAG                 = '@' [a-zA-Z]+ ('-' [a-zA-Z0-9]+)* ;

	whitespace              = [ \t] ;
}%%
