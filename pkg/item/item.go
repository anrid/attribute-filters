package item

import (
	"fmt"
	"strconv"
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
	Attributes   map[string][]string
}

func (b *ItemsBatch) Add(rec, headers []string) error {
	isItem := len(rec) == 8 && headers[2] == "status"
	if !isItem {
		return fmt.Errorf("does not look like an Item record: %+v", headers)
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

	if pairs, found := b.Attributes[i.ID]; found {
		i.Attributes = pairs
	}

	b.Total++

	if b.Total <= 10 {
		fmt.Printf("Preview item: %v\n", rec)
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
	return nil
}

func ToInt64(n string) int64 {
	i, err := strconv.ParseInt(n, 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}
