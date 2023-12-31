package main

import (
	"os"

	"github.com/anrid/attribute-filters/pkg/attribute"
	"github.com/anrid/attribute-filters/pkg/elastic"
	"github.com/spf13/pflag"
)

func main() {
	itemsDir := pflag.StringP("items-dir", "d", "", "dir containing Item files in gzipped CSV format [REQUIRED]")
	attributesDir := pflag.StringP("attributes-dir", "a", "", "dir containing Item Attribute database exported from Postgres [REQUIRED]")
	categoriesFile := pflag.StringP("categories-file", "c", "", "JSON file containing Categories [REQUIRED]")
	prefixFilter := pflag.StringP("filename-prefix-filter", "f", "items", "filename prefix to match on the given Items dir")
	batchSize := pflag.Int("batch-size", 5000, "batch size, i.e. number of items to insert into ES at a time")
	max := pflag.Int("max", 20_000, "process max X items before exiting")

	pflag.Parse()

	if *itemsDir == "" {
		pflag.PrintDefaults()
		os.Exit(-1)
	}

	db := attribute.NewDB()

	if *itemsDir == "" || *attributesDir == "" || *categoriesFile == "" {
		pflag.PrintDefaults()
		os.Exit(-1)
	}

	err := db.LoadCategoriesJSON(*categoriesFile)
	if err != nil {
		panic(err)
	}

	err = db.ImportPostgresDatabase(attribute.ImportPostgresDatabaseArgs{Dir: *attributesDir})
	if err != nil {
		panic(err)
	}

	elastic.Index(elastic.IndexArgs{
		Dir:          *itemsDir,
		PrefixFilter: *prefixFilter,
		Max:          *max,
		BatchSize:    *batchSize,
		ConvertIDs:   db.IDs,
	})

	// res, err := elastic.Query(elastic.QueryArgs{
	// 	C: &elastic.Conditions{
	// 		Keyword: "iphone 15",
	// 	},
	// })
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Printf("query result:\n%+v\n", res)
}
