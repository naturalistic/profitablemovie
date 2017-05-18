package profitablemovie

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
	elastic "gopkg.in/olivere/elastic.v5"
)

const avgGrossAggName = "avgGrossAgg"
const colorString = "Color"		// string used to indicate colour in import movie csv file
const configFileName = "config.json"
const listSeparator = "|"		// list separator in import movie csv file
const termsAggName = "termsAgg"
const yearsAggName = "yearsAgg"
const movieFieldCount = 28		// field count in import movie csv file

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

type imdbMovie struct {
	Actor1FacebookLikes int		`json:"actor1FacebookLikes,omitempty"`
	Actor1Name string		`json:"actor1Name"`
	Actor2FacebookLikes int		`json:"actor2FacebookLikes"`
	Actor2Name string		`json:"actor2Name"`
	Actor3FacebookLikes int		`json:"actor3FacebookLikes"`
	Actor3Name string		`json:"actor3Name"`
	AspectRatio float64		`json:"aspectRatio"`
	BudgetUSD int			`json:"budgetUSD"`
	CastTotalFacebookLikes int	`json:"castTotalFacebookLikes"`
	ContentRating string		`json:"contentRating"`
	Country string			`json:"country"`
	CriticCount int			`json:"criticCount"`
	DirectorFacebookLikes int	`json:"directorFacebookLikes"`
	DirectorName string		`json:"directorName"`
	DurationMinutes int		`json:"durationMinutes"`
	FaceNumberInPoster int		`json:"faceNumberInPoster"`
	Genres []string			`json:"genres"`
	GrossUSD int			`json:"grossUSD"`
	IMDBScore float64		`json:"imdbScore"`
	IsColor bool			`json:"isColor"`
	Language string			`json:"language"`
	MovieFacebookLikes int		`json:"movieFacebookLikes"`
	MovieIMDBLink string		`json:"movieIMDBLink"`
	MovieTitle string		`json:"movieTitle"`
	PlotKeywords []string		`json:"plotKeywords"`
	TitleYear string		`json:"titleYear"`
	UserReviewCount int		`json:"userReviewCount"`
	VotedUsersCount int		`json:"votedUsersCount"`
}

var keywordMap = map[string] searchParameters {
	"movie_gross_by_country.csv" : {"country.keyword", 3, 20 },
	"movie_gross_by_genre.csv" : {"genres.keyword", 6, 30},
}

// Accepts a filename of a potential csv data file to update. If it's a recognised data file, first checks if already
// cached. If it isn't or the cached file is expired the data file will be updated via. elastic search.
func UpdateData(filename string) error {
	config, err := getConfig()
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

// Iterates over a provided CSV file containing a list of movies and inserts into an Elastic Cluster & Index
func ImportMovies(filepath string, overwrite bool) error {
	config, err := getConfig()
	if err != nil {
		return err
	}
	client, err := getElasticClient(config, overwrite)
	if err != nil {
		return err
	}
	ctx := context.Background()
	file, err := os.Open(filepath)	// Open CSV containing movies
	if err != nil {
		return err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = ','

	firstLine := true
	movieCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if firstLine  {
			firstLine = false
			continue
		}
		movieCount++
		movie, err := parseMovie(record)
		if err != nil {
			return err
		}
		_, err = client.Index().
			Index(config.IndexName).
			Type(config.TypeName).
			Id(string(movieCount)).
			BodyJson(movie).
			Do(ctx)

		if err != nil {
			return err
		}
		fmt.Printf("Added movie '%s'\n", movie.MovieTitle)
	}
	fmt.Printf("Added %d movies to index");
	return nil
}

func getConfig() (*dataManagerConfig, error) {
	file, err := ioutil.ReadFile(configFileName)
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
	client, err := getElasticClient(config, false)
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
func getElasticClient(config *dataManagerConfig, overwrite bool) (*elastic.Client, error) {
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
	if exists && overwrite {
		client.DeleteIndex(config.IndexName).Do(ctx)
	}
	if !exists || overwrite {
		createIndex, err := client.CreateIndex(config.IndexName).Do(ctx)
		if err != nil {
			return nil, err
		}
		if !createIndex.Acknowledged {
			return nil, errors.New("DataManager: Unable to determine if elastic index created")
		}
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

func parseMovie(record []string) (movie *imdbMovie, err error) {
	if len(record) != movieFieldCount {
		return nil, errors.New("DataManager: unable to parse csv due to invalid field count")
	}
	m := imdbMovie {}
	m.Actor1FacebookLikes, _ = strconv.Atoi(record[7])
	m.Actor1Name = record[10]
	m.Actor2FacebookLikes, _ = strconv.Atoi(record[24])
	m.Actor2Name = record[6]
	m.Actor3FacebookLikes, _ = strconv.Atoi(record[5])
	m.Actor3Name = record[14]
	m.AspectRatio, _ = strconv.ParseFloat(record[26], 64)
	m.BudgetUSD, _ = strconv.Atoi(record[22])
	m.CastTotalFacebookLikes, _ = strconv.Atoi(record[13])
	m.ContentRating = record[21]
	m.Country = record[20]
	m.CriticCount, _ = strconv.Atoi(record[2])
	m.DirectorFacebookLikes, _ = strconv.Atoi(record[4])
	m.DirectorName = record[1]
	m.DurationMinutes, _ = strconv.Atoi(record[3])
	m.FaceNumberInPoster, _ = strconv.Atoi(record[15])
	m.Genres = strings.Split(record[9], listSeparator)
	m.GrossUSD, _ = strconv.Atoi(record[8])
	m.IMDBScore, _ = strconv.ParseFloat(record[25], 64)
	m.IsColor = record[0] == colorString
	m.Language = record[19]
	m.MovieFacebookLikes, _ = strconv.Atoi(record[27])
	m.MovieIMDBLink = record[17]
	m.MovieTitle = record[11]
	m.PlotKeywords = strings.Split(record[16], listSeparator)
	m.TitleYear = record[23]
	m.UserReviewCount, _ = strconv.Atoi(record[18])
	m.VotedUsersCount, _ = strconv.Atoi(record[12])
	return &m, nil
}

