package main

import (
	"fmt"
	"os"

	"github.com/anrid/attribute-filters/pkg/elastic"
	"github.com/spf13/pflag"
)

func main() {
	itemsDir := pflag.StringP("items-dir", "d", "", "dir containing Item files in gzipped CSV format [REQUIRED]")
	filenameFilter := pflag.StringP("filename-filter", "f", ".csv.gz", "filename pattern to filter on in data dir")
	batchSize := pflag.Int("batch-size", 5000, "batch size, i.e. number of items to insert into ES at a time")
	max := pflag.Int("max", 20_000, "process max X items before exiting")

	pflag.Parse()

	if *itemsDir == "" {
		pflag.PrintDefaults()
		os.Exit(-1)
	}

	elastic.Index(elastic.IndexArgs{
		Dir:            *itemsDir,
		FilenameFilter: *filenameFilter,
		Max:            *max,
		BatchSize:      *batchSize,
	})

	res, err := elastic.Query(elastic.QueryArgs{
		C: &elastic.Conditions{
			Keyword: "iphone 15",
		},
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("query result:\n%+v\n", res)
}
