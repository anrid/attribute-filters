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
	dataDir := pflag.String("data", "", "Dir with gzipped CSV files containing exported tables from the Item Attributes Postgres database (e.g. attributes.csv.gz)")
	categoriesFile := pflag.StringP("cats", "c", "", "Item categories file (.json.gz format)")
	dumpCategoryRule := pflag.Int("dump", 242, "Dump rule for category ID X")

	pflag.Parse()

	if *dataDir == "" || *categoriesFile == "" {
		pflag.PrintDefaults()
		os.Exit(-1)
	}

	db := attribute.NewDB()

	err := db.LoadCategoriesJSON(*categoriesFile)
	if err != nil {
		panic(err)
	}

	err = db.ImportPostgresDatabase(attribute.ImportPostgresDatabaseArgs{Dir: *dataDir})
	if err != nil {
		panic(err)
	}

	// Force garbage collection
	runtime.GC()
	time.Sleep(250 * time.Millisecond)

	db.Dump(attribute.DumpOpts{
		OnlyCategoryID:  *dumpCategoryRule,
		MaxLinesToPrint: 100,
	})

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
