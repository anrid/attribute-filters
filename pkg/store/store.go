package store

import (
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/mus-format/mus-go/varint"
)

var (
	PrimaryKey = []byte("primary-key")
)

type Store struct {
	db *badger.DB
	pk *badger.Sequence
}

func New() *Store {
	return new(Store)
}

func uint64ToBytes(v uint64) (bs []byte) {
	size := varint.SizeUint64(v)
	bs = make([]byte, size)
	varint.MarshalUint64(v, bs)
	return
}

func (s *Store) SetBytes(pk uint64, bytes []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(uint64ToBytes(pk), bytes)
		err := txn.SetEntry(e)
		return err
	})
}

func (s *Store) GetBytes(pk uint64) (bytes []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(uint64ToBytes(pk))
		if err != nil {
			return err
		}

		return item.Value(func(bs []byte) error {
			bytes = append([]byte{}, bs...)
			return nil
		})
	})

	return
}

func (s *Store) PrimaryKey() (num uint64) {
	var err error
	if s.pk == nil {
		s.pk, err = s.db.GetSequence(PrimaryKey, 1000)
		if err != nil {
			log.Panic(err)
		}
	}

	retries := 10

	for i := 0; i < retries; i++ {
		num, err = s.pk.Next()
		if err != nil {
			fmt.Printf("WARN: s.pk.Next() error: %s\n", err)
			s.pk, err = s.db.GetSequence(PrimaryKey, 1000)
			if err != nil {
				log.Panic(err)
			}
		} else {
			break
		}
	}

	return
}

func (s *Store) Connect(path string) (close func()) {
	// Open the Badger database located in directory /path.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions(path)
	opts.IndexCacheSize = 100 << 20 // 100 mb

	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Panic(err)
	}

	s.db = db

	return func() {
		if s.pk != nil {
			s.pk.Release()
		}

		for {
			err := s.db.RunValueLogGC(0.7)
			if err != nil {
				break
			}
		}

		s.db.Close()
	}
}
