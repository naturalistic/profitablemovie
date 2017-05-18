// Based on Golang code documentation https://golang.org

package main

import (
	"encoding/json"
	"fmt"
	"flag"
	"github.com/naturalistic/profitablemovie/datamanager"
	"io/ioutil"
	"net/http"
	"regexp"
	"text/template"
)

const templateFilename = "view.html"
const templatePath = "website/" + templateFilename

type Page struct {
	DataFile string		`json:"data_file"`
	Heading string		`json:"heading"`
	LayerType string	`json:"layer_type"`
	NavItems string		`json:"nav_items"`
}

func loadPage(title string) (*Page, error) {
	filename := "pages/" + title + ".json"
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
	err = profitablemovie.UpdateData(p.DataFile)
	if(err != nil) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	renderTemplate(w, templateFilename, p)
}

var templates = template.Must(template.ParseFiles(templatePath))

func renderTemplate(w http.ResponseWriter, tmpl string, p *Page) {
	err := templates.ExecuteTemplate(w, tmpl, p)
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
	loadDataPtr := flag.String("loaddata", "", "Provide the path of a CSV data file to load into " +
		"the index specified in config.json")
	overwritePtr := flag.Bool("overwrite", false, "Overwrite existing index when loading data")
	flag.Parse()

	if len(*loadDataPtr) != 0 {
		err := profitablemovie.ImportMovies(*loadDataPtr, *overwritePtr)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Movies imported successfully")
		}
		return
	}
	fs := http.FileServer(http.Dir("website"))
	http.Handle("/website/", http.StripPrefix("/website/", fs))
	http.Handle("/", makeHandler(viewHandler))
	fmt.Println("Listening...")
	http.ListenAndServe(":8080", nil)
}
