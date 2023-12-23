package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/anrid/attribute-filters/pkg/attribute"
	"github.com/spf13/pflag"
)

func main() {
	dataDir := pflag.StringP("data", "d", "", "Dir with gzipped CSV files containing exported tables from the Item Attributes Postgres database (e.g. attributes.csv.gz)")
	categoriesFile := pflag.StringP("cats", "c", "", "Item categories file in JSON format")
	expandDB := pflag.Int("expand-db", 0, "Import the same Postgres data <X> times, effectively making the attributes DB <X> times larger")
	dumpCategoryRule := pflag.Int("dump", 242, "Dump rule for category ID X")

	pflag.Parse()

	db := attribute.NewDB()

	if *dataDir == "" || *categoriesFile == "" {
		pflag.PrintDefaults()
		os.Exit(-1)
	}

	err := db.LoadCategoriesJSON(*categoriesFile)
	if err != nil {
		panic(err)
	}

	if *expandDB > 0 {
		// Import the same Postgres database X times effectively making the DB X times larger
		// Used for load testing purposes
		for i := 0; i < *expandDB; i++ {
			db.ForceAppendSuffixToAllConvertedKeys(fmt.Sprintf("-expanded-%03d", i))

			err := db.ImportPostgresDatabase(attribute.ImportPostgresDatabaseArgs{Dir: *dataDir})
			if err != nil {
				panic(err)
			}

			// Force garbage collection
			runtime.GC()
			time.Sleep(250 * time.Millisecond)
		}
	} else {
		// Import data normally
		err := db.ImportPostgresDatabase(attribute.ImportPostgresDatabaseArgs{Dir: *dataDir})
		if err != nil {
			panic(err)
		}
	}

	db.PreSort()

	if *dumpCategoryRule > 0 {
		db.Dump(attribute.DumpOpts{
			OnlyCategoryID:  *dumpCategoryRule,
			MaxLinesToPrint: 100,
		})
	}

	PrintMemUsage()

	time.Sleep(time.Second)
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the
// number of garage collection cycles completed. For info on each,
// see: https://golang.org/pkg/runtime/#MemStats
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc      = %v MiB\n", m.Alloc/1024/1024)
	fmt.Printf("TotalAlloc = %v MiB\n", m.TotalAlloc/1024/1024)
	fmt.Printf("Sys        = %v MiB\n", m.Sys/1024/1024)
	fmt.Printf("NumGC      = %v\n", m.NumGC)
}
