// Based on Golang code documentation https://golang.org

package main

import (
	"text/template"
	"io/ioutil"
	"net/http"
	"regexp"
	"encoding/json"
)

type Page struct {
	NavItems string		`json:"nav_items"`
	Heading string		`json:"heading"`
	DataFile string		`json:"data_file"`
	LayerType string	`json:"layer_type"`
}

func loadPage(title string) (*Page, error) {
	filename := "templates/" + title + ".json"
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var page Page
	err = json.Unmarshal(file,&page)
	if err != nil {
		return nil, err
	}
	return &page, nil
}

func viewHandler(w http.ResponseWriter, r *http.Request, title string) {
	p, err := loadPage(title)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	renderTemplate(w, "view", p)
}

var templates = template.Must(template.ParseFiles("view.html"))

func renderTemplate(w http.ResponseWriter, tmpl string, p *Page) {
	err := templates.ExecuteTemplate(w, tmpl+".html", p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

var validPath = regexp.MustCompile("^/([a-zA-Z0-9-]+)$")

func makeHandler(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m := validPath.FindStringSubmatch(r.URL.Path)
		if m == nil {
			http.NotFound(w, r)
			return
		}
		fn(w, r, m[1])
	}
}

func main() {
	fs := http.FileServer(http.Dir("website"))
	http.Handle("/website/", http.StripPrefix("/website/", fs))
	http.Handle("/", makeHandler(viewHandler))
	http.ListenAndServe(":8080", nil)
}
