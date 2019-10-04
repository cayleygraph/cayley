package http

import (
	"html/template"
	"net/http"

	"github.com/gobuffalo/packr"
	"github.com/julienschmidt/httprouter"
)

var templatesBox = packr.NewBox("../../templates")
var t *template.Template

func setupUI() {
	templateDirectory := templatesBox.List()
	for _, file := range templateDirectory {
		data, _ := templatesBox.FindString(file)
		if t == nil {
			t = template.New(file)
		} else {
			t = t.New(file)
		}
		_, err := t.Parse(data)
		if err != nil {
			panic(err)
		}
	}
}

func serveUI(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	uiType := params.ByName("ui_type")
	if r.URL.Path == "/" {
		uiType = "query"
	}
	err := t.ExecuteTemplate(w, uiType+".html", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
