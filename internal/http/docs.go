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

package http

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/russross/blackfriday"
)

type DocRequestHandler struct {
	assets string
}

func MarkdownWithCSS(input []byte, title string) []byte {
	// set up the HTML renderer
	htmlFlags := 0
	htmlFlags |= blackfriday.HTML_USE_XHTML
	htmlFlags |= blackfriday.HTML_USE_SMARTYPANTS
	htmlFlags |= blackfriday.HTML_SMARTYPANTS_FRACTIONS
	htmlFlags |= blackfriday.HTML_SMARTYPANTS_LATEX_DASHES
	htmlFlags |= blackfriday.HTML_COMPLETE_PAGE
	renderer := blackfriday.HtmlRenderer(htmlFlags, title, markdownCSS)

	// set up the parser
	extensions := 0
	//extensions |= blackfriday.EXTENSION_NO_INTRA_EMPHASIS
	extensions |= blackfriday.EXTENSION_TABLES
	extensions |= blackfriday.EXTENSION_FENCED_CODE
	extensions |= blackfriday.EXTENSION_AUTOLINK
	extensions |= blackfriday.EXTENSION_STRIKETHROUGH
	//extensions |= blackfriday.EXTENSION_SPACE_HEADERS
	extensions |= blackfriday.EXTENSION_HEADER_IDS
	extensions |= blackfriday.EXTENSION_LAX_HTML_BLOCKS

	return blackfriday.Markdown(input, renderer, extensions)
}

func (h *DocRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	docpage := params.ByName("docpage")
	if docpage == "" {
		docpage = "Index"
	}
	if !strings.HasSuffix(docpage, ".md") {
		docpage += ".md"
	}
	file, err := os.Open(fmt.Sprintf("%s/docs/%s", h.assets, docpage))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNoContent)
		return
	}
	output := MarkdownWithCSS(data, fmt.Sprintf("Cayley Docs - %s", docpage))
	fmt.Fprint(w, string(output))
}

var markdownCSS = "/static/css/docs.css"
