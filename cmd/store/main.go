package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/anrid/attribute-filters/pkg/item"
	"github.com/anrid/attribute-filters/pkg/serialize"
	"github.com/anrid/attribute-filters/pkg/store"
)

func main() {
	s := store.New()

	close := s.Connect("../test-data/badger-db")
	defer close()

	pk1 := s.PrimaryKey()
	pk2 := s.PrimaryKey()
	pk3 := s.PrimaryKey()

	fmt.Printf("pk: %d\n", pk1)
	fmt.Printf("pk: %d\n", pk2)
	fmt.Printf("pk: %d\n", pk3)

	b := []byte(fmt.Sprintf("%d-%d-%d", pk1+10, pk2+10, pk3+10))
	err := s.SetBytes(pk2, b)
	if err != nil {
		panic(err)
	}

	b2, err := s.GetBytes(pk2)
	if err != nil {
		panic(err)
	}

	fmt.Printf("got stored value for pk %d : %s\n", pk2, string(b2))

	item := &item.Item{
		ID:            fmt.Sprintf("item_%d", pk3),
		Name:          "This is a Pen. I am A BOI!",
		Created:       time.Now().UnixMilli(),
		Updated:       time.Now().UnixMilli(),
		Status:        item.StatusOnSale,
		ItemCondition: item.ItemConditionGood,
		CategoryID:    242,
		Price:         1_100,
		Attributes:    []string{"a1-o10", "a2-o20", "a3-o155"},
	}
	itemBytes, _ := serialize.ItemToBytes(item)

	err = s.SetBytes(pk3, itemBytes)
	if err != nil {
		panic(err)
	}
	fmt.Printf("stored %d bytes in pk %d\n", len(itemBytes), pk3)

	itemBytes2, err := s.GetBytes(pk3)
	if err != nil {
		panic(err)
	}
	fmt.Printf("got %d bytes for pk %d\n", len(itemBytes2), pk3)

	item2, err := serialize.ItemFromBytes(itemBytes2)
	if err != nil {
		panic(err)
	}

	fmt.Printf("got stored item for pk %d :\n%s\n", pk3, ToPrettyJSON(item2))
}

func ToPrettyJSON(o interface{}) string {
	b, _ := json.MarshalIndent(o, "", "  ")
	return string(b)
}
