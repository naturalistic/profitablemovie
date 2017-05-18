package profitablemovie

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"
	"time"
	elastic "gopkg.in/olivere/elastic.v5"
)

const avgGrossAggName = "avgGrossAgg"
const configFileName = "config.json"
const termsAggName = "termsAgg"
const yearsAggName = "yearsAgg"

type dataManagerConfig struct {
	CacheExpiryMinutes int	`json:"cache_expiry_minutes"`
	ClusterUrl string	`json:"cluster_url"`
	DataPath string		`json:"data_path"`
	TypeName string		`json:"type_name"`
	IndexName string	`json:"index_name"`
}

type searchParameters struct {
	Keyword string
	TermCount int
	YearCount int
}

var keywordMap = map[string] searchParameters {
	"movie_gross_by_country.csv" : {"country.keyword", 3, 20 },
	"movie_gross_by_genre.csv" : {"genres.keyword", 6, 30},
}

// Accepts a filename of a potential csv data file to update. If it's a recognised data file, first checks if already
// cached. If it isn't or the cached file is expired the data file will be updated via. elastic search.
func UpdateData(filename string) error {
	config, err := loadConfig(configFileName)
	if err != nil {
		return err
	}
	if sp, ok := keywordMap[filename]; ok {
		filepath := config.DataPath + filename
		info, err := os.Stat(filepath)
		if err != nil || time.Since(info.ModTime()).Minutes() >= float64(config.CacheExpiryMinutes) {
			sr, err := performSearch(sp, config)
			if err != nil {
				return err
			}
			return writeSearchResult(sr, filepath)
		}
		return nil // file up-to-date
	}
	return errors.New("DataManager: no updater associated with given filename: " + filename)
}

func loadConfig(filename string) (*dataManagerConfig, error) {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config dataManagerConfig
	err = json.Unmarshal(file,&config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// PerformsSearch performs an elastic search terms query. It accepts a Keyword to use as the main terms query, the Term
// Count (takes top ranked by frequency) & the YearCount to go back. Returns data grouped by the term, then year with
// avg. gross for each
func performSearch(sp searchParameters, config *dataManagerConfig) (*elastic.SearchResult, error) {
	client, err := getElasticClient(config)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()

	search := client.Search().Index(config.IndexName).Type(config.TypeName)

	termsAgg := elastic.NewTermsAggregation().Field(sp.Keyword)
	termsAgg = termsAgg.Order("_count", false).Size(sp.TermCount)

	search = search.Aggregation(termsAggName, termsAgg)

	yearsAgg := elastic.NewTermsAggregation().Field("titleYear.keyword")
	yearsAgg = yearsAgg.Order("_term", false).Size(sp.YearCount)

	avgGrossAgg := elastic.NewAvgAggregation().Field("grossUSD")

	yearsAgg = yearsAgg.SubAggregation(avgGrossAggName, avgGrossAgg)
	termsAgg = termsAgg.SubAggregation(yearsAggName, yearsAgg)

	return search.Do(ctx)
}

// Initialises and returns an instance of the elastic client
func getElasticClient(config *dataManagerConfig) (*elastic.Client, error) {
	ctx := context.Background()
	client, err := elastic.NewClient(elastic.SetURL(config.ClusterUrl), elastic.SetSniff(false))
	if err != nil {
		return nil, err
	}
	_, _, err = client.Ping(config.ClusterUrl).Do(ctx)
	if err != nil {
		return nil, err
	}
	exists, err := client.IndexExists(config.IndexName).Do(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("DataManager: Index " + config.ClusterUrl + " does not exist")
	}
	return client, nil
}

// WriteSearchResult accepts a elastic search result grouped by a term & year with a value for each year
// Iterates through the data and writes a csv file to the filename provided
func writeSearchResult(sr *elastic.SearchResult, filepath string) error {
	records := [][]string{
		{ "key", "value", "date" },
	}

	if agg, found := sr.Aggregations.Terms(termsAggName); found {
		for _, bucket := range agg.Buckets {
			str, ok := bucket.Key.(string)
			if !ok {
				return errors.New("DataManager: Couldn't interpret search result")
			}

			if agg2, found := bucket.Terms(yearsAggName); found {
				for _, bucket2 := range agg2.Buckets {
					str2, ok := bucket2.Key.(string)
					if !ok {
						return errors.New("DataManager: Couldn't interpret search result")
					}
					if agg3, found := bucket2.Avg(avgGrossAggName); found {
						records = append(records, []string{str,fmt.Sprintf("%.0f", *agg3.Value), str2})
					}
				}
			}
		}
	}
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	w.WriteAll(records) // calls Flush internally

	if err := w.Error(); err != nil {
		return err
	}
	return nil
}