package importer

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	DebugPrint = true
)

type AddFunc = func(rec, headers []string) error

type Batcher interface {
	Add(rec, headers []string) error
	Flush() error
}

type FromGzippedCSVFilesArgs struct {
	Dir              string  // Dir to look for files in
	PrefixFilter     string  // limit to filenames matching the filter
	MaxRecordsToRead int     // Max CSV records to read before exiting
	Batcher          Batcher // Use this batcher (optional)
	AddFunc          AddFunc // Call this add function for each record (optional)
}

func FromGzippedCSVFiles(a FromGzippedCSVFilesArgs) {
	dir, err := os.ReadDir(a.Dir)
	if err != nil {
		log.Panic(err)
	}

	var total int

	for _, fi := range dir {
		if !strings.HasPrefix(fi.Name(), a.PrefixFilter) {
			continue
		}

		fmt.Printf("Reading CSV records from file: %s\n", fi.Name())

		filename := filepath.Join(a.Dir, fi.Name())
		f, err := os.Open(filename)
		if err != nil {
			log.Panic(err)
		}

		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Panic(err)
		}

		cr := csv.NewReader(gr)
		var records int
		var exitEarly bool
		var headers []string

		for {
			rec, err := cr.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Panic(err)
			}

			records++

			if records == 1 {
				headers = rec
				continue
			}
			if DebugPrint {
				if records == 2 {
					// First record
					for i, value := range rec {
						fmt.Printf(" - %02d  %-40s  : %-30s\n", i, headers[i], value)
					}
				}
			}

			total++

			if a.Batcher != nil {
				err = a.Batcher.Add(rec, headers)
				if err != nil {
					log.Panic(err)
				}
			} else if a.AddFunc != nil {
				err = a.AddFunc(rec, headers)
				if err != nil {
					log.Panic(err)
				}
			} else {
				log.Panicf("missing both batcher and add function")
			}

			if DebugPrint {
				if total%100_000 == 0 {
					fmt.Printf("Read %d records ..\n", total)
				}
			}

			if a.MaxRecordsToRead > 0 && total >= a.MaxRecordsToRead {
				exitEarly = true
				break
			}
		}

		if a.Batcher != nil {
			err = a.Batcher.Flush()
			if err != nil {
				log.Panic(err)
			}
		}

		f.Close()

		if exitEarly {
			break
		}
	}

	fmt.Printf("Read %d records total\n", total)
}
