package main

import (
	"fmt"
	"os"
	"time"

	"github.com/anrid/attribute-filters/pkg/attribute"
	"github.com/anrid/attribute-filters/pkg/elastic"
	"github.com/spf13/pflag"
)

func main() {
	attributesDir := pflag.StringP("attributes-data", "a", "", "dir with gzipped CSV files containing exported tables from the Item Attributes Postgres database (e.g. attributes.csv.gz)")
	categoriesFile := pflag.String("categories-file", "", "categories file in JSON format")

	keyword := pflag.StringP("keyword", "k", "", "keyword/phrase to search for")
	categoryID := pflag.IntP("cid", "c", 0, "limit to category ID")
	max := pflag.IntP("max", "m", 3, "return max X items")

	pflag.Parse()

	if *keyword == "" && *categoryID == 0 {
		pflag.PrintDefaults()
		os.Exit(-1)
	}

	cond := new(elastic.Conditions)
	if *keyword != "" {
		cond.Keyword = *keyword
	}
	if *categoryID > 0 {
		cond.CategoryIDs = append(cond.CategoryIDs, *categoryID)
	}

	res, err := elastic.Query(elastic.QueryArgs{
		C:               cond,
		Size:            *max,
		CategoryFacets:  true,
		AttributeFacets: true,
	})
	if err != nil {
		panic(err)
	}

	hasResults := (len(res.Items) > 0 || len(res.CategoryFacets) > 0 || len(res.AttributeFacets) > 0)
	lookupFilesAvalable := *attributesDir != "" && *categoriesFile != ""

	if hasResults && lookupFilesAvalable {
		db := attribute.NewDB()

		err = db.LoadCategoriesJSON(*categoriesFile)
		if err != nil {
			panic(err)
		}

		err = db.ImportPostgresDatabase(attribute.ImportPostgresDatabaseArgs{Dir: *attributesDir})
		if err != nil {
			panic(err)
		}

		fmt.Printf("\nQuery results:\n\n")

		tpl := "%03d. Item %s (score: %2.02f)\n" +
			" - name      : %s\n" +
			" - category  : %s\n" +
			" - created   : %s\n" +
			" - status    : %d\n"

		for c, i := range res.Items {
			created := time.UnixMilli(i.Created)
			fmt.Printf(tpl,
				c+1,
				i.ID,
				res.Scores[c],
				i.Name,
				db.FullCategoryName(i.CategoryID),
				created.Format("2006-01-02 15:04:05"),
				i.Status,
			)

			if len(i.Attributes) > 0 {
				fmt.Printf(" - attributes:\n")
				for _, pair := range i.Attributes {
					fmt.Printf("    - [%-13s]  %s\n", pair, db.AttributeOptionPairToString(pair))
				}
			}

			fmt.Println("")
		}

		fmt.Println("")
	} else {
		fmt.Printf("Query result:\n%s\n", elastic.ToPrettyJSON(res))
	}
}
