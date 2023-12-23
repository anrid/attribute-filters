package elastic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/anrid/attribute-filters/pkg/attribute"
	"github.com/anrid/attribute-filters/pkg/importer"
	"github.com/anrid/attribute-filters/pkg/item"
	"github.com/bytedance/sonic"
	"github.com/ikawaha/kagome-dict/ipa"
	"github.com/ikawaha/kagome/v2/tokenizer"
)

const (
	Host                 = "http://127.0.0.1:9200"
	ItemsNoDescIndexName = "items_no_desc"
	DebugPrint           = true
)

type Map = map[string]interface{}

type IndexArgs struct {
	Dir            string
	FilenameFilter string
	BatchSize      int
	Max            int
}

func Index(a IndexArgs) {
	fmt.Printf("Running indexer: max %d items ..\n", a.Max)

	CreateIndex()

	start := time.Now()

	importer.FromGzippedCSVFiles(importer.FromGzippedCSVFilesArgs{
		Dir:          a.Dir,
		PrefixFilter: a.FilenameFilter,
		Batcher: &item.ItemsBatch{
			Size:         a.BatchSize,
			ForEachBatch: BulkIndex,
		},
		MaxRecordsToRead: a.Max,
	})

	Refresh(ItemsNoDescIndexName)
	stats := IndexStats(ItemsNoDescIndexName)

	fmt.Printf("Index stats (after):\n%s\n", ToPrettyJSON(stats))
	fmt.Printf("Finished indexing %d items in %s\n", stats.All.Primaries.Docs.Count, time.Since(start))
}

type Conditions struct {
	Keyword     string
	CategoryIDs []int
	Statuses    []item.Status
	Attributes  []*attribute.AttributeCondition
}

type QueryResult struct {
	TotalHits       int
	Size            int
	From            int
	Items           []*item.Item
	ItemIDs         []string
	CategoryFacets  []*CategoryFacet
	AttributeFacets []*AttributeFacet
}

type CategoryFacet struct {
	CategoryID int
	Count      int
}

type AttributeFacet struct {
	AttributeOptionPair string
	Count               int
}

type QueryArgs struct {
	C                *Conditions
	DoNotFetchSource bool
	From             int
	Size             int
	CategoryFacets   bool
	AttributeFacets  bool
}

var (
	compactWhitespace = regexp.MustCompile(`[ 　]{1,}`)
)

func Query(a QueryArgs) (*QueryResult, error) {
	if a.Size == 0 {
		a.Size = 10
	}

	createdSort := Map{"created": "desc"}
	scoreSort := Map{"_score": "desc"}
	var sort *Map

	boolQuery := Map{}
	filterTerms := []Map{}

	if len(a.C.CategoryIDs) > 0 {
		filterTerms = append(filterTerms, Map{"terms": Map{"category_id": a.C.CategoryIDs}})
	}
	if len(a.C.Statuses) > 0 {
		filterTerms = append(filterTerms, Map{"terms": Map{"status": a.C.Statuses}})
	}
	if len(filterTerms) > 0 {
		boolQuery["filter"] = filterTerms
		sort = &createdSort
	}

	if a.C.Keyword != "" {
		boolQuery["should"] = []Map{
			{"match": Map{"name": Map{"query": a.C.Keyword}}},
		}
		boolQuery["minimum_should_match"] = 1
		sort = &scoreSort
	}

	esQuery := Map{
		"query": Map{
			"bool": boolQuery,
		},
		"size":    a.Size,
		"_source": !a.DoNotFetchSource,
		"from":    a.From,
	}
	if sort != nil {
		esQuery["sort"] = *sort
	}

	aggs := Map{}
	if a.CategoryFacets {
		aggs["category_facets"] = Map{"terms": Map{"field": "category_id"}}
	}
	if a.AttributeFacets {
		aggs["attribute_facets"] = Map{"terms": Map{"field": "attributes"}}
	}
	if len(aggs) > 0 {
		esQuery["aggs"] = aggs
	}

	if DebugPrint {
		fmt.Printf("Query:\n%s\n", ToPrettyJSON(esQuery))
	}

	res, code, err := Call(http.MethodPost, Host+"/"+ItemsNoDescIndexName+"/_search", ToJSON(esQuery))
	if err != nil {
		return nil, err
	}
	if code >= 300 {
		fmt.Printf("Query dump:\n=====================\n%s\n", ToPrettyJSON(esQuery))
		return nil, fmt.Errorf("got unexpected status code %d : %s", code, res)
	}

	// preview := res[:]
	// if len(preview) > 500 {
	// 	preview = preview[0:500]
	// }
	// fmt.Printf("Preview: %s\n", preview)

	se := new(SearchResult)
	err = sonic.Unmarshal(res, se)
	if err != nil {
		return nil, err
	}
	if DebugPrint {
		fmt.Printf("Search Result:\n%s\n", ToPrettyJSON(se))
	}

	qr := &QueryResult{
		TotalHits: int(se.Hits.Total.Value),
		Size:      a.Size,
		From:      a.From,
	}

	if se.Hits.Hits != nil {
		for i, doc := range se.Hits.Hits {
			if doc.Source != nil {
				s := doc.Source
				name := compactWhitespace.ReplaceAllString(s.Name, " ")

				fmt.Printf(
					"%03d. [Score: %2.02f] ID: %s Name: '%s' Status: %d Category: %d\n",
					i+1, doc.Score, s.ID, name, s.Status, s.CategoryID,
				)

				qr.Items = append(qr.Items, &item.Item{
					ID:            s.ID,
					Name:          name,
					Status:        s.Status,
					Created:       s.Created,
					Updated:       s.Updated,
					CategoryID:    s.CategoryID,
					Price:         s.Price,
					ItemCondition: s.ItemCondition,
					Attributes:    s.Attributes,
				})
			} else {
				fmt.Printf("%03d. [Score: %2.02f] ID: %s\n", i+1, doc.Score, doc.ID)

				qr.ItemIDs = append(qr.ItemIDs, doc.ID)
			}
		}
	}
	if se.Aggs != nil {
		if a.CategoryFacets {
			if f, found := se.Aggs["category_facets"]; found {
				for _, b := range f.Buckets {
					qr.CategoryFacets = append(qr.CategoryFacets, &CategoryFacet{
						CategoryID: int(b.Key.(float64)),
						Count:      b.DocCount,
					})
				}
			}
		}
		if a.AttributeFacets {
			if f, found := se.Aggs["attribute_facets"]; found {
				for _, b := range f.Buckets {
					qr.AttributeFacets = append(qr.AttributeFacets, &AttributeFacet{
						AttributeOptionPair: b.Key.(string),
						Count:               b.DocCount,
					})
				}
			}
		}
	}

	return qr, nil
}

type SearchResult struct {
	Took int64 `json:"took"` // 2

	Hits struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			Index  string     `json:"_index"` // "test"
			ID     string     `json:"_id"`    // "102"
			Score  float64    `json:"_score"` // 10.781843
			Source *item.Item `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`

	Aggs map[string]*Aggs `json:"aggregations"`
}

type Aggs struct {
	DocCountErrorUpperBound int `json:"doc_count_error_upper_bound"` // : 0,
	SumOtherDocCount        int `json:"sum_other_doc_count"`         // : 0,
	Buckets                 []struct {
		Key      interface{} `json:"key"`
		DocCount int         `json:"doc_count"`
	} `json:"buckets"`
}

func BulkIndex(itemsTotal int, items []*item.Item) error {
	tok := KagomeV2Tokenizer()

	for _, i := range items {
		name := tok.Wakati(i.Name)
		i.Name = strings.Join(name, " ")
	}

	var docs []interface{}
	for _, i := range items {
		docs = append(docs, Map{"index": Map{"_index": ItemsNoDescIndexName, "_id": i.ID}})
		docs = append(docs, i)
	}

	bulk := BuildBulkBody(docs...)
	if len(bulk) > 10_000_000 {
		fmt.Printf("WARNING: bulk index body is %d bytes large!\n", len(bulk))
	}

	fmt.Printf("Bulk indexing %d items (JSON payload: %d bytes)\n", len(items), len(bulk))
	res, code, err := Call(http.MethodPost, Host+"/_bulk", bulk)
	if err != nil {
		return fmt.Errorf("bulk index error: %s", err)
	}

	if code != http.StatusOK {
		fmt.Printf("Got status code %d - res: %+v\n", code, res)
		return fmt.Errorf("bulk index: got status code %d", code)
	}

	return nil
}

func CreateIndex() {
	res, code, err := Call(http.MethodDelete, Host+"/"+ItemsNoDescIndexName, nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	res, code, err = Call(http.MethodPut, Host+"/"+ItemsNoDescIndexName, ToJSON(Map{
		"mappings": Map{
			"properties": Map{
				"id":             Map{"type": "keyword"},
				"name":           Map{"type": "text"},
				"status":         Map{"type": "integer"},
				"created":        Map{"type": "date", "format": "epoch_millis"},
				"updated":        Map{"type": "date", "format": "epoch_millis"},
				"category_id":    Map{"type": "integer"},
				"item_condition": Map{"type": "integer"},
				"attributes":     Map{"type": "keyword"},
			},
		},
		"settings": Map{
			"number_of_shards": 1,
			"index": Map{
				"queries.cache.enabled": "true",
				"similarity": Map{
					"default": Map{
						"type": "BM25",
						"b":    0.75,
						"k1":   1.2,
					},
				},
			},
		},
	}))
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}
}

type ESIndexStats struct {
	All struct {
		Primaries struct {
			Docs struct {
				Count int64 `json:"count"`
			} `json:"docs"`
			Store struct {
				SizeInBytes             int64 `json:"size_in_bytes"`
				TotalDataSetSizeInBytes int64 `json:"total_data_set_size_in_bytes"`
			} `json:"store"`
			QueryCache struct {
				CacheCount     int64 `json:"cache_count"`
				CacheSize      int64 `json:"cache_size"`
				Evictions      int64 `json:"evictions"`
				HitCount       int64 `json:"hit_count"`
				MemSizeInBytes int64 `json:"memory_size_in_bytes"`
				MissCount      int64 `json:"miss_count"`
				TotalCount     int64 `json:"total_count"`
			} `json:"query_cache"`
			RequestCache struct {
				Evictions      int64 `json:"evictions"`
				HitCount       int64 `json:"hit_count"`
				MemSizeInBytes int64 `json:"memory_size_in_bytes"`
				MissCount      int64 `json:"miss_count"`
			} `json:"request_cache"`
		} `json:"primaries"`
	} `json:"_all"`
}

func IndexStats(index string) *ESIndexStats {
	res, _, err := Call(http.MethodGet, Host+"/"+index+"/_stats", nil)
	if err != nil {
		log.Panic(err)
	}

	// if DebugPrint {
	// 	all := make(map[string]interface{})
	// 	err = sonic.Unmarshal(res, &all)
	// 	if err != nil {
	// 		log.Panic(err)
	// 	}
	// 	fmt.Printf("All Stats:\n%s\n\n", ToPrettyJSON(all))
	// }

	stats := new(ESIndexStats)
	err = sonic.Unmarshal(res, stats)
	if err != nil {
		log.Panic(err)
	}

	return stats
}

func Refresh(index string) {
	res, code, err := Call(http.MethodGet, Host+"/"+index+"/_refresh", nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}
}

func BuildBulkBody(obs ...interface{}) (bulk []byte) {
	for _, o := range obs {
		bulk = append(bulk, ToJSON(o)...)
		bulk = append(bulk, []byte("\n")...)
	}
	bulk = append(bulk, []byte("\n")...)
	return
}

func Call(method, url string, body []byte) (respBody []byte, statusCode int, err error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Add("content-type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	statusCode = resp.StatusCode

	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return
}

var _t *tokenizer.Tokenizer

func KagomeV2Tokenizer() *tokenizer.Tokenizer {
	if _t == nil {
		var err error
		_t, err = tokenizer.New(ipa.Dict(), tokenizer.OmitBosEos())
		if err != nil {
			log.Panic(err)
		}
	}
	return _t
}

func ToJSON(o interface{}) []byte {
	b, err := sonic.Marshal(o)
	if err != nil {
		log.Panic(err)
	}
	return b
}

func ToPrettyJSON(o interface{}) []byte {
	b, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		log.Panic(err)
	}
	return b
}
