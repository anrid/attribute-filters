package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/anrid/attribute-filters/pkg/attribute"
	"github.com/spf13/pflag"
)

func main() {
	dataDir := pflag.StringP("data", "d", "", "Dir with gzipped CSV files containing exported tables from the Item Attributes Postgres database (e.g. attributes.csv.gz)")
	categoriesFile := pflag.StringP("cats", "c", "", "Item categories file (.json.gz format)")
	selectedCategoryID := pflag.Int("cid", 242, "Selected category ID")
	selectedAttributes := pflag.StringSliceP("attrs", "a", []string{}, "Selected attributes")
	pageSize := pflag.Int("page", 3, "Page size / max number of options to return per attribute")

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

	db.PreSort()

	db.Dump(attribute.DumpOpts{
		OnlyCategoryID:  *selectedCategoryID,
		MaxLinesToPrint: 100,
	})

	sc := &attribute.SearchConditions{
		CategoryIDs: []int{*selectedCategoryID},
		PageSize:    *pageSize,
	}
	if len(*selectedAttributes) > 0 {
		for _, s := range *selectedAttributes {
			parts := strings.SplitN(s, "-", 2)

			attributeID, _ := strconv.Atoi(parts[0])
			optionID, _ := strconv.Atoi(parts[1])

			sc.Attributes = append(sc.Attributes, &attribute.AttributeCondition{
				AttributeID: attributeID,
				OptionID:    optionID,
			})
		}
	}

	res, err := attribute.FindVisibleAttributes(sc, db)
	if err != nil {
		panic(err)
	}

	fmt.Println("")

	for _, va := range res.VAs {
		fmt.Printf(" - attribute [%-6d] - %s\n", va.ID, va.Title)
		for _, vo := range va.Os {
			fmt.Printf("    - option [%-6d] - %s\n", vo.ID, vo.Title)
		}
	}

	fmt.Println("")

	toPrettyJSON(sc)
}

func toPrettyJSON(o interface{}) {
	b, _ := json.MarshalIndent(o, "", "  ")
	fmt.Printf("%s\n", string(b))
}
