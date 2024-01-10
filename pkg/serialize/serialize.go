package serialize

import (
	"github.com/anrid/attribute-filters/pkg/item"
	"github.com/mus-format/mus-go"
	"github.com/mus-format/mus-go/ord"
	"github.com/mus-format/mus-go/varint"
)

func MarshalItem(i *item.Item, bs []byte) (n int) {
	m := mus.MarshallerFn[string](ord.MarshalString)
	n = ord.MarshalSlice[string](i.Attributes, m, bs)

	n += varint.MarshalInt(i.CategoryID, bs[n:])
	n += varint.MarshalInt(i.Price, bs[n:])
	n += varint.MarshalInt64(i.Created, bs[n:])
	n += varint.MarshalInt64(i.Updated, bs[n:])
	n += varint.MarshalInt(int(i.ItemCondition), bs[n:])
	n += varint.MarshalInt(int(i.Status), bs[n:])
	n += ord.MarshalString(i.ID, bs[n:])
	n += ord.MarshalString(i.Name, bs[n:])

	return
}

func UnmarshalItem(bs []byte) (i *item.Item, n int, err error) {
	i = new(item.Item)

	u := mus.UnmarshallerFn[string](ord.UnmarshalString)
	i.Attributes, n, err = ord.UnmarshalSlice[string](u, bs)
	if err != nil {
		return
	}

	var n1 int
	i.CategoryID, n1, err = varint.UnmarshalInt(bs[n:])
	n += n1
	if err != nil {
		return
	}
	i.Price, n1, err = varint.UnmarshalInt(bs[n:])
	n += n1
	if err != nil {
		return
	}
	i.Created, n1, err = varint.UnmarshalInt64(bs[n:])
	n += n1
	if err != nil {
		return
	}
	i.Updated, n1, err = varint.UnmarshalInt64(bs[n:])
	n += n1
	if err != nil {
		return
	}
	_itemCondition, n1, err := varint.UnmarshalInt(bs[n:])
	n += n1
	i.ItemCondition = item.ItemCondition(_itemCondition)
	if err != nil {
		return
	}
	_status, n1, err := varint.UnmarshalInt(bs[n:])
	n += n1
	i.Status = item.Status(_status)
	if err != nil {
		return
	}
	i.ID, n1, err = ord.UnmarshalString(bs[n:])
	n += n1
	if err != nil {
		return
	}
	i.Name, n1, err = ord.UnmarshalString(bs[n:])
	n += n1
	return
}

func SizeItem(i *item.Item) (size int) {
	s := mus.SizerFn[string](ord.SizeString)
	size = ord.SizeSlice[string](i.Attributes, s)
	size += varint.SizeInt(i.CategoryID)
	size += varint.SizeInt(i.Price)
	size += varint.SizeInt64(i.Created)
	size += varint.SizeInt64(i.Updated)
	size += varint.SizeInt(int(i.ItemCondition))
	size += varint.SizeInt(int(i.Status))
	size += ord.SizeString(i.ID)
	size += ord.SizeString(i.Name)
	return
}

func ItemToBytes(i *item.Item) (bs []byte, n int) {
	size := SizeItem(i)
	bs = make([]byte, size)
	n = MarshalItem(i, bs)
	return
}

func ItemFromBytes(bs []byte) (i *item.Item, err error) {
	i, _, err = UnmarshalItem(bs)
	return
}

func bytesToStringSlice(bs []byte) (values []string, n int, err error) {
	u := mus.UnmarshallerFn[string](ord.UnmarshalString)
	values, n, err = ord.UnmarshalSlice[string](u, bs)
	return
}
