package attribute

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/anrid/attribute-filters/pkg/importer"
)

const (
	DebugPrint = false
)

type DB struct {
	IDs           map[string]int        `json:"ids"`
	IDCounter     int                   `json:"id_counter"`
	Attributes    map[int]*Attribute    `json:"attributes"`
	Options       map[int]*Option       `json:"options"`
	CategoryRules map[int]*CategoryRule `json:"category_rules"` // key = category_id
	ReverseIDs    map[int]string        `json:"reverse_ids"`
	CategoryTree  map[int]*Category     `json:"category_tree"` // key = category_id

	tmpCategoryAttrRels []*tmpCategoryAttributeRel `json:"-"`
	tmpDynOptRels       []*tmpDynamicOptionRel     `json:"-"`

	// This can be used to load the same data over and over
	// to stresstest the attribute database, e.g. to ensure
	// that it performs well even with 100x the data loaded.
	appendSuffixToConvertedKeys string `json:"-"`
}

func NewDB() *DB {
	db := new(DB)

	db.IDs = make(map[string]int)
	db.Attributes = make(map[int]*Attribute)
	db.Options = make(map[int]*Option)
	db.CategoryRules = make(map[int]*CategoryRule)
	db.ReverseIDs = make(map[int]string)
	db.CategoryTree = make(map[int]*Category)

	return db
}

func (db *DB) ForceAppendSuffixToAllConvertedKeys(suffix string) {
	db.appendSuffixToConvertedKeys = suffix
}

func (db *DB) LoadCategoriesJSON(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &db.CategoryTree)
	if err != nil {
		return err
	}

	fmt.Printf("Loaded %d categories\n", len(db.CategoryTree))
	c1 := db.CategoryTree[5]
	c2 := db.CategoryTree[1409]

	if DebugPrint {
		fmt.Printf("cat id: %6d  %+v\n", c1.ID, c1)
		fmt.Printf("cat id: %6d  %+v\n", c2.ID, c2)
	}

	return nil
}

func (db *DB) FullCategoryName(categoryID int) string {
	c := db.CategoryTree[categoryID]
	var name []string
	for _, id := range c.Path {
		pc := db.CategoryTree[id]
		name = append(name, pc.Name)
	}
	name = append(name, c.Name)
	return strings.Join(name, " - ")
}

func (db *DB) AttributeOptionPairToString(pair string) string {
	parts := strings.SplitN(pair, "-", 2)
	attributeID, _ := strconv.Atoi(parts[0])
	optionID, _ := strconv.Atoi(parts[1])

	a := db.Attributes[attributeID]
	o := db.Options[optionID]

	return a.Title + " - " + o.Title
}

// Takes a CSV file (gzipped) containing a ItemID->AttributeID->AttributeValue
// relationships and converts them to use this DB's IDs (integer primary keys).
func (db *DB) ConvertItemAttributeRelationships(fromGzippedCSVFile, toCSVFile string) {
	start := time.Now()

	fmt.Printf("Relationships CSV file : %s\n", fromGzippedCSVFile)
	fmt.Printf("Writing to CSV file    : %s\n\n", toCSVFile)

	fr, err := os.Open(fromGzippedCSVFile)
	if err != nil {
		panic(err)
	}
	defer fr.Close()

	fw, err := os.OpenFile(toCSVFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer fw.Close()

	gr, err := gzip.NewReader(fr)
	if err != nil {
		panic(err)
	}

	cr := csv.NewReader(gr)
	cw := csv.NewWriter(fw)
	defer cw.Flush()

	err = cw.Write([]string{"item_id", "attribute_to_option_pairs"})
	if err != nil {
		panic(err)
	}

	var line int
	var headers []string
	var lastItemID string
	var buffer []int
	var items, attrToOptionPairs int

	sourceCounts := make(map[string]int)
	missingOptions := make(map[int]int)

	for {
		rec, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		line++

		if line == 1 {
			// Headers
			headers = rec
			continue
		}

		if line == 2 {
			// First record
			for i, value := range rec {
				fmt.Printf(" - %02d  %-40s  : %-30s\n", i, headers[i], value)
			}
		}

		itemID := rec[0]
		attributeUUID := rec[1]
		attributeValue := rec[2]
		attributeSource := rec[3]

		attributeID, found := db.IDs[attributeUUID]
		if !found {
			fmt.Printf("WARN: could not find attribute UUID %s\n", attributeUUID)
			continue
		}

		a, found := db.Attributes[attributeID]
		if !found {
			log.Panicf("could not find attribute %d\n", attributeID)
		}
		if !a.IsSearchable || a.IsDisabled {
			continue
		}
		if attributeValue == "" {
			fmt.Printf("WARN: item %s attribute [%6d] has empty value\n", itemID, a.ID)
			continue
		}

		// Valid attribute values are UUIDs (e.g. 4b220b12-6bdf-5a22-a66b-bc89db2e48c5)
		// pointing to an option
		optionUUID := attributeValue
		if len(optionUUID) > 36 {
			// Skip non-UUID values
			continue
		}

		optionID, found := db.IDs[optionUUID]
		if !found {
			if false {
				fmt.Printf(
					"WARN: item %s could not find option %s for attribute [%6d] - %s\n",
					itemID, optionUUID, a.ID, a.Title,
				)
			}
			missingOptions[a.ID]++
			continue
		}

		o, found := db.Options[optionID]
		if !found {
			log.Panicf("could not find option %d\n", optionID)
		}

		if lastItemID != "" && lastItemID != itemID {
			// Store!
			var aoRels []string
			for i := 0; i < len(buffer); i += 2 {
				aoRels = append(aoRels, strconv.Itoa(buffer[i])+"-"+strconv.Itoa(buffer[i+1]))
			}
			buffer = nil

			err = cw.Write([]string{itemID, strings.Join(aoRels, "|")})
			if err != nil {
				panic(err)
			}

			items++
		}

		buffer = append(buffer, a.ID, o.ID)
		attrToOptionPairs++

		sourceCounts[attributeSource]++

		lastItemID = itemID
	}

	fmt.Printf("Finished loading relationship data in %s\n", time.Since(start))
	fmt.Printf("Read %d records (found %d items and %d attribute_to_option_pairs)\n\n", line, items, attrToOptionPairs)
	fmt.Printf("Attribute source counts:\n%s\n", ToPrettyJSON(sourceCounts))
	fmt.Printf("Missing options:\n%s\n\n", ToPrettyJSON(missingOptions))
}

func ToPrettyJSON(o interface{}) string {
	b, _ := json.MarshalIndent(o, "", "  ")
	return string(b)
}

type ImportPostgresDatabaseArgs struct {
	Dir string
}

// Dir contains gzipped CSV of key tables from the Item Attributes database
// exported from Postgres (each table exported as a separate CSV file).
func (db *DB) ImportPostgresDatabase(a ImportPostgresDatabaseArgs) error {
	start := time.Now()

	importer.FromGzippedCSVFiles(importer.FromGzippedCSVFilesArgs{
		Dir:          a.Dir,
		PrefixFilter: "attribute.csv.gz",
		AddFunc:      db.AddAttribute,
	})

	importer.FromGzippedCSVFiles(importer.FromGzippedCSVFilesArgs{
		Dir:          a.Dir,
		PrefixFilter: "attribute_option.csv.gz",
		AddFunc:      db.AddOption,
	})

	importer.FromGzippedCSVFiles(importer.FromGzippedCSVFilesArgs{
		Dir:          a.Dir,
		PrefixFilter: "category_attribute.csv.gz",
		AddFunc:      db.AddCategoryAttribute,
	})

	importer.FromGzippedCSVFiles(importer.FromGzippedCSVFilesArgs{
		Dir:          a.Dir,
		PrefixFilter: "dynamic_attribute_option.csv.gz",
		AddFunc:      db.AddDynamicOption,
	})

	fmt.Printf("Finished loading data in %s\n", time.Since(start))

	return db.PostProcessImportedData()
}

func (db *DB) Attribute(id int) *Attribute {
	a, found := db.Attributes[id]
	if !found {
		log.Panicf("could not find attribute %d (uuid: %s)", id, db.ReverseIDs[id])
	}
	return a
}

func (db *DB) Option(id int) *Option {
	o, found := db.Options[id]
	if !found {
		log.Panicf("could not find option %d (uuid: %s)", id, db.ReverseIDs[id])
	}
	return o
}

func (db *DB) CategoryRule(categoryID int) *CategoryRule {
	r, found := db.CategoryRules[categoryID]
	if !found {
		log.Panicf("could not find rule for category %d", categoryID)
	}
	return r
}

func (db *DB) PostProcessImportedData() error {
	start := time.Now()

	// Create a map that can reverse int IDs back to their original UUID strings
	for uuid, id := range db.IDs {
		db.ReverseIDs[id] = uuid
	}

	// Create references between attributes and options
	for _, o := range db.Options {
		a := db.Attribute(o.AttributeID)
		a.OptionIDs = append(a.OptionIDs, o.ID)
	}

	// Create category rules.
	// These rules define which attributes and options are visible
	// for a given category
	for _, r := range db.tmpCategoryAttrRels {
		if r.IsDisabled {
			continue
		}

		a := db.Attribute(r.AttributeID)

		rule, found := db.CategoryRules[r.CategoryID]
		if !found {
			rule = &CategoryRule{
				CategoryID:             r.CategoryID,
				ShowIfOptionIDSelected: make(map[int][]*LimitedOptions),
				ShowOptionIDAlways:     make(map[int]bool),
			}
			db.CategoryRules[rule.CategoryID] = rule
		}

		rule.AttributeIDs = append(rule.AttributeIDs, a.ID)
	}

	// Add dynamic options logic to category rules
	for _, r := range db.tmpDynOptRels {
		if r.IsDisabled {
			continue
		}

		o := db.Option(r.OptionID)
		rule := db.CategoryRule(r.CategoryID)

		if len(r.Precondition) < 3 {
			// No precondition, this option is always visible
			var isValid bool
			for _, id := range rule.AttributeIDs {
				if id == o.AttributeID {
					isValid = true
					break
				}
			}

			if !isValid {
				if DebugPrint {
					fmt.Printf(
						"WARN: attribute %d NOT found among category %d attributes %v (dynamic_attribute_option_id %s)\n",
						o.AttributeID, r.CategoryID, rule.AttributeIDs, r.OriginalUUID,
					)
				}
				continue
			}

			rule.ShowOptionIDAlways[o.ID] = true

		} else {
			// At least 1 precondition, this option is visible when precondition met
			// e.g. when the required option is selected
			preconds := strings.SplitN(strings.Trim(r.Precondition, "{}"), ",", -1)

			for i := 0; i < len(preconds); i++ {
				pcUUID := preconds[i]

				if len(pcUUID) != 36 {
					if len(pcUUID) > 36 && len(pcUUID) < 50 {
						// There are '¥' chars found appended to some UUIDs - WTF?!
						pcUUID = strings.Trim(pcUUID, "¥")
					} else if len(pcUUID) == 72 {
						// Some UUID strings actually contain 2 UUID concatenated
						tmp1 := pcUUID[0:36]
						tmp2 := pcUUID[36:]
						pcUUID = tmp1
						preconds = append(preconds, tmp2)
					} else {
						if DebugPrint {
							fmt.Printf("WARN: invalid precondition UUID %s - skipping!\n", pcUUID)
						}
						continue
					}
				}

				pcID := db.ConvertID(pcUUID)

				if pcA, found := db.Attributes[pcID]; found {
					// Precondition is an attribute
					if DebugPrint {
						fmt.Printf(
							"WARN: found attribute %d (title: %s  uuid: %s) as precondition for dynamic_attribute_option ID %d (title: %s , uuid: %s) - does this make sense?!\n",
							pcA.ID, pcA.Title, db.ReverseIDs[pcA.ID], o.ID, o.Title, db.ReverseIDs[o.ID],
						)
					}
				} else if pcO, found := db.Options[pcID]; found {
					// Precondition is an option
					rule.AddLimitedOption(pcO.ID, o)
				} else {
					// Precondition was neither an attribute or an option
					if DebugPrint {
						fmt.Printf("WARN: could not find attribute or option for precondition %d (uuid: %s)\n", pcID, pcUUID)
					}
				}
			}
		}
	}

	// Determine which attributes show always be visible for each
	// category rule
	for _, rule := range db.CategoryRules {
		// Get all attributes with options that can get limited (hidden)
		// when a certain options are selected
		limitedAttributeIDs := make(map[int]bool)
		for _, los := range rule.ShowIfOptionIDSelected {
			for _, lo := range los {
				limitedAttributeIDs[lo.AttributeID] = true
			}
		}

		for _, id := range rule.AttributeIDs {
			if !limitedAttributeIDs[id] {
				// This attribute is always visible
				rule.AlwaysVisibleAttributeIDs = append(rule.AlwaysVisibleAttributeIDs, id)
			}
		}
	}

	// Clear tmp data
	db.tmpCategoryAttrRels = nil
	db.tmpDynOptRels = nil

	fmt.Printf("Finished post-processing data in %s\n", time.Since(start))

	return nil
}

func (db *DB) AddAttribute(rec, headers []string) error {
	// - 00  attribute_id                              : 754abe74-304e-4925-b61a-52aa58eb8153
	// - 01  attribute_type                            : enum
	// - 02  is_multiple_allowed                       : f
	// - 03  is_required                               : f
	// - 04  is_disabled                               : f
	// - 05  title                                     : ヒール高さ
	// - 06  display_order                             : 3
	// - 07  created_at                                : 2023-04-24 03:01:33.473284+00
	// - 08  updated_at                                : 2023-06-06 04:16:32.368978+00
	// - 09  searchable                                : f
	// - 10  listing_type                              : single_select
	// - 11  display_page                              : 2
	// - records: 7742
	o := new(Attribute)

	o.ID = db.ConvertID(rec[0])
	o.Type = rec[1]
	if rec[2] == "t" {
		o.IsMultipleAllowed = true
	}
	if rec[3] == "t" {
		o.IsRequired = true
	}
	if rec[4] == "t" {
		o.IsDisabled = true
	}
	o.Title = rec[5]
	if rec[6] != "" {
		o.DisplayOrder = atoi(rec[6])
	}
	if rec[9] == "t" {
		o.IsSearchable = true
	}
	o.ListingType = rec[10]
	o.DisplayPage = atoi(rec[11])

	db.Attributes[o.ID] = o

	return nil
}

func (db *DB) AddOption(rec, headers []string) error {
	// - 00  attribute_option_id                       : 3ce5f8be-4b0a-45bf-a44e-9f90aab5edc3
	// - 01  attribute_id                              : 1dda946b-44b9-46a7-aec6-abc4a1ee3f26
	// - 02  title                                     : S 拡張パック 漆黒のガイスト
	// - 03  is_disabled                               : f
	// - 04  display_order                             : 8
	// - 05  created_at                                : 2023-04-24 03:01:33.473284+00
	// - 06  updated_at                                : 2023-05-22 07:18:16.318604+00
	// - 07  color                                     : {"red":0,"green":0,"blue":0}
	// - 08  subtitle                                  :
	// - records: 173820
	o := new(Option)

	o.ID = db.ConvertID(rec[0])

	if rec[1] == "0" || rec[1] == "" {
		if DebugPrint {
			fmt.Printf("WARN: empty attribute_id for attribute_option %s - skipping!\n", rec[0])
		}
		return nil
	}
	o.AttributeID = db.ConvertID(rec[1])

	o.Title = rec[2]
	if rec[3] == "t" {
		o.IsDisabled = true
	}
	if rec[4] != "" {
		o.DisplayOrder = atoi(rec[4])
	}
	o.Color = rec[7]
	o.Subtitle = rec[8]

	db.Options[o.ID] = o

	return nil
}

func (db *DB) AddCategoryAttribute(rec, headers []string) error {
	// - 00  category_attribute_id                     : 4da81626-45ae-4106-b450-0262e1a1fcd5
	// - 01  category_id                               : 135
	// - 02  attribute_id                              : b6b9be89-648c-4af9-b3fe-1b77de4833d1
	// - 03  is_disabled                               : f
	// - 04  created_at                                : 2023-04-24 03:01:33.473284+00
	// - 05  updated_at                                : 2023-04-24 03:01:33.473284+00
	// - records: 8978
	o := new(tmpCategoryAttributeRel)

	if rec[1] == "0" || rec[1] == "" {
		if DebugPrint {
			fmt.Printf("WARN: empty category_id for category_attribute %s - skipping!\n", rec[0])
		}
		return nil
	}
	o.CategoryID = atoi(rec[1]) // category id is already an int!

	if rec[2] == "0" || rec[2] == "" {
		if DebugPrint {
			fmt.Printf("WARN: empty attribute_id for category_attribute %s - skipping!\n", rec[0])
		}
		return nil
	}
	o.AttributeID = db.ConvertID(rec[2])

	if rec[3] == "t" {
		o.IsDisabled = true
	}

	db.tmpCategoryAttrRels = append(db.tmpCategoryAttrRels, o)

	return nil
}

func (db *DB) AddDynamicOption(rec, headers []string) error {
	// - 00  dynamic_attribute_option_id               : 6d94fa0b-653e-4ceb-bb98-d2becfc9caa6
	// - 01  category_id                               : 179
	// - 02  attribute_option_id                       : a4bf6fbb-4fd4-4417-bd62-cb05629c9b31
	// - 03  precondition                              : {5508f4a4-7369-4993-904f-7fab6ae31305}
	// - 04  is_disabled                               : f
	// - 05  created_at                                : 2023-09-11 02:33:17.06964+00
	// - 06  updated_at                                : 2023-09-11 02:33:17.06964+00
	// - records: 73673
	o := new(tmpDynamicOptionRel)

	if rec[1] == "0" || rec[1] == "" {
		if DebugPrint {
			fmt.Printf("WARN: empty category_id for dynamic_attribute_option %s - skipping!\n", rec[0])
		}
		return nil
	}
	o.CategoryID = atoi(rec[1]) // category id is already an int!

	if rec[2] == "0" || rec[2] == "" {
		if DebugPrint {
			fmt.Printf("WARN: empty attribute_option_id for dynamic_attribute_option %s - skipping!\n", rec[0])
		}
		return nil
	}
	o.OptionID = db.ConvertID(rec[2]) // can be empty!

	o.Precondition = rec[3]
	if rec[4] == "t" {
		o.IsDisabled = true
	}

	o.OriginalUUID = rec[0] // To help with debugging / validation

	db.tmpDynOptRels = append(db.tmpDynOptRels, o)

	return nil
}

func (db *DB) ConvertID(uuid string) (id int) {
	if uuid == "" {
		log.Panicln("got empty uuid")
	}
	if db.appendSuffixToConvertedKeys != "" {
		uuid += db.appendSuffixToConvertedKeys
	}

	var found bool
	id, found = db.IDs[uuid]
	if !found {
		db.IDCounter++
		id = db.IDCounter
		db.IDs[uuid] = id
	}

	return
}

func (db *DB) PreSort() {
	start := time.Now()

	fmt.Printf("Pre-sorting ...\n")

	// Sort attributes by display order for all category rules
	for _, rule := range db.CategoryRules {
		if len(rule.AttributeIDs) > 1 {
			sort.SliceStable(rule.AttributeIDs, func(i, j int) bool {
				a1 := db.Attribute(rule.AttributeIDs[i])
				a2 := db.Attribute(rule.AttributeIDs[j])
				return a1.DisplayOrder < a2.DisplayOrder
			})
		}

		if len(rule.AlwaysVisibleAttributeIDs) > 1 {
			sort.SliceStable(rule.AlwaysVisibleAttributeIDs, func(i, j int) bool {
				a1 := db.Attribute(rule.AlwaysVisibleAttributeIDs[i])
				a2 := db.Attribute(rule.AlwaysVisibleAttributeIDs[j])
				return a1.DisplayOrder < a2.DisplayOrder
			})
		}

		// For each attribute, sort attribute options by display order
		// for _, id := range rule.AttributeIDs {
		// 	if len(a.Options) > 1 {
		// 		sort.SliceStable(a.Options, func(i, j int) bool {
		// 			return a.Options[i].DisplayOrder < a.Options[j].DisplayOrder
		// 		})
		// 	}
		// }
	}

	fmt.Printf("Finished pre-sorting data in %s\n", time.Since(start))
}

func atoi(n string) int {
	i, err := strconv.Atoi(n)
	if err != nil {
		panic(err)
	}
	return i
}

type Attribute struct {
	ID                int
	Type              string
	IsMultipleAllowed bool
	IsRequired        bool
	IsDisabled        bool
	Title             string
	DisplayOrder      int
	IsSearchable      bool
	ListingType       string
	DisplayPage       int
	OptionIDs         []int
}

type Option struct {
	ID           int
	AttributeID  int
	Title        string
	IsDisabled   bool
	DisplayOrder int
	Color        string
	Subtitle     string
}

type CategoryRule struct {
	CategoryID                int
	ShowIfOptionIDSelected    map[int][]*LimitedOptions // key = option ID (from precondition)
	ShowOptionIDAlways        map[int]bool              // key = option ID
	AttributeIDs              []int
	AlwaysVisibleAttributeIDs []int
}

func (r *CategoryRule) AddLimitedOption(selectedOptionID int, o *Option) {
	los, found := r.ShowIfOptionIDSelected[selectedOptionID]
	if !found {
		r.ShowIfOptionIDSelected[selectedOptionID] = []*LimitedOptions{
			{AttributeID: o.AttributeID, OptionIDs: []int{o.ID}},
		}
	} else {
		var appended bool
		for _, lo := range los {
			if lo.AttributeID == o.AttributeID {
				lo.OptionIDs = append(lo.OptionIDs, o.ID)
				appended = true
				break
			}
		}
		if !appended {
			los = append(los, &LimitedOptions{AttributeID: o.AttributeID, OptionIDs: []int{o.ID}})
			r.ShowIfOptionIDSelected[selectedOptionID] = los
		}
	}
}

type LimitedOptions struct {
	AttributeID int
	OptionIDs   []int
}

type tmpCategoryAttributeRel struct {
	CategoryID  int
	AttributeID int
	IsDisabled  bool
}

type tmpDynamicOptionRel struct {
	OriginalUUID string
	CategoryID   int
	OptionID     int
	Precondition string
	IsDisabled   bool
}

type Category struct {
	// E.g. map[id:1165 name:テント/タープ order:50 parent_id:1164 path:[8 1164]]
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Order    int    `json:"order"`
	ParentID int    `json:"parent_id"`
	Path     []int  `json:"path"`
}
