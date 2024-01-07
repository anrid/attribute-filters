package item

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
)

type Status int

const (
	StatusOnSale Status = iota + 1
	StatusTrading
	StatusSold
	StatusStopped
	StatusCancel
	StatusOther
)

type ItemCondition int

const (
	ItemConditionLikeNew ItemCondition = iota + 1
	ItemConditionGood
	ItemConditionPoor
	ItemConditionOther
)

type Item struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	Status        Status        `json:"status"`
	Created       int64         `json:"created"`
	Updated       int64         `json:"updated"`
	CategoryID    int           `json:"category_id"`
	Price         int           `json:"price"`
	ItemCondition ItemCondition `json:"item_condition"`
	Attributes    []string      `json:"attributes"`
}

type ItemsBatch struct {
	Size         int
	Total        int
	Items        []*Item
	ForEachBatch func(totalItems int, items []*Item) error
	ConvertIDs   map[string]int // key = UUID, value = int (attribute ID or option ID)
	stats        map[string]map[string]int
}

func (b *ItemsBatch) Add(rec, headers []string) error {
	isItem := len(rec) == 9 && headers[2] == "status"
	if !isItem {
		return fmt.Errorf("does not look like an Item record: %+v", headers)
	}

	// Tally up some basic stats for this import
	{
		if b.stats == nil {
			b.stats = make(map[string]map[string]int)
		}

		if _, found := b.stats["statuses"]; !found {
			b.stats["statuses"] = make(map[string]int)
		}
		b.stats["statuses"][rec[2]]++

		if _, found := b.stats["categories"]; !found {
			b.stats["categories"] = make(map[string]int)
		}
		b.stats["categories"][rec[5]]++
	}

	i := new(Item)

	i.ID = rec[0]
	i.Name = rec[1]
	switch rec[2] {
	case "on_sale":
		i.Status = StatusOnSale
	case "trading":
		i.Status = StatusTrading
	case "sold_out":
		i.Status = StatusSold
	case "stop":
		i.Status = StatusStopped
	case "cancel":
		i.Status = StatusCancel
	default:
		i.Status = StatusOther
	}
	i.Created = ToInt64(rec[3])
	i.Updated = ToInt64(rec[4])
	i.CategoryID = int(ToInt64(rec[5]))
	i.Price = int(ToInt64(rec[6]))
	switch rec[7] {
	case "1":
		i.ItemCondition = ItemConditionLikeNew
	case "2":
		i.ItemCondition = ItemConditionGood
	case "3":
		i.ItemCondition = ItemConditionPoor
	default:
		i.ItemCondition = ItemConditionOther
	}

	if len(rec[8]) > 1 {
		// This record contains item attribute-option pairs
		aoPairs := strings.SplitN(rec[8], "|", -1)
		for _, aoPair := range aoPairs {
			parts := strings.SplitN(aoPair, "=", 2)
			if len(parts) != 2 {
				log.Panicf("Failed to parse invalid attributes data: '%s'\n", rec[8])
			}

			attributeUUID := parts[0]
			optionUUID := parts[1]

			attributeID, found := b.ConvertIDs[attributeUUID]
			if !found {
				// fmt.Printf("WARN: could not find attribute %s for item %s\nattributes data: %s\n", attributeUUID, ToPrettyJSON(i), rec[8])
				continue
			}

			optionID, found := b.ConvertIDs[optionUUID]
			if !found {
				// fmt.Printf("could not find option %s for item %s\nattributes data: %s\n", optionUUID, ToPrettyJSON(i), rec[8])
				continue
			}

			pair := fmt.Sprintf("%d-%d", attributeID, optionID)
			i.Attributes = append(i.Attributes, pair)
		}
	}

	b.Total++

	if b.Total <= 10 {
		fmt.Printf("preview record:\n")
		for j, h := range headers {
			fmt.Printf(" - %s: %s\n", h, rec[j])
		}
		fmt.Println("")
	}

	b.Items = append(b.Items, i)

	if len(b.Items) >= b.Size {
		err := b.ForEachBatch(b.Total, b.Items)
		if err != nil {
			return err
		}
		b.Items = nil
	}
	return nil
}

func (b *ItemsBatch) Flush() error {
	if len(b.Items) > 0 {
		err := b.ForEachBatch(b.Total, b.Items)
		if err != nil {
			return err
		}
		b.Items = nil
	}

	fmt.Printf(
		"Items batch reader stats:\n%s\nFound %d unique categories\n\n",
		ToPrettyJSON(b.stats["statuses"]), len(b.stats["categories"]),
	)

	return nil
}

func ToInt64(n string) int64 {
	i, err := strconv.ParseInt(n, 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}

func ToPrettyJSON(o interface{}) string {
	b, _ := json.MarshalIndent(o, "", "  ")
	return string(b)
}
