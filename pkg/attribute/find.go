package attribute

import (
	"fmt"
	"math"
	"strings"
)

func FindVisibleAttributes(sc *SearchConditions, db *DB) (res *FindVisibleAttributesResponse, err error) {
	res = new(FindVisibleAttributesResponse)
	res.VAs = make([]*VisibleAttribute, 0)
	res.Corrected = new(SearchConditions)

	if len(sc.CategoryIDs) > 1 {
		err = fmt.Errorf("UNIMPLEMENTED: cannot handle multiple category IDs yet")
		return
	}
	if len(sc.CategoryIDs) == 0 {
		// Clear all attributes
		return
	}

	if sc.PageSize == 0 {
		// Default page size is 100
		sc.PageSize = 100
	}
	if sc.PageSize > 1_000 {
		// Limit page size to 1,000 options per attribute max
		sc.PageSize = 1_000
	}
	res.PageSize = sc.PageSize

	catID := sc.CategoryIDs[0]

	res.Corrected.CategoryIDs = []int{catID}
	res.Corrected.PageSize = sc.PageSize
	res.Corrected.Offset = sc.Offset
	res.Corrected.Filters = sc.Filters

	selectedAs := make(map[int]bool)
	selectedOs := make(map[int]bool)

	for _, ac := range sc.Attributes {
		selectedAs[ac.AttributeID] = true
		selectedOs[ac.OptionID] = true
	}

	rule := db.CategoryRule(catID)

	// TODO: Handle multiple option filters
	var filter *OptionFilter
	if len(sc.Filters) > 0 {
		filter = sc.Filters[0]
	}

	visibleAs := make(map[int]*VisibleAttribute)
	visibleOs := make(map[int]bool)

	res.Pages = 1

	// Add visible attributes
	for _, attributeID := range rule.AlwaysVisibleAttributeIDs {
		a := db.Attribute(attributeID)

		va := &VisibleAttribute{
			ID:    a.ID,
			Title: a.Title,
		}

		// All options for this attribute should be visible
		// but we return max X options per page
		var offset int
		var count int

		if len(a.OptionIDs) > sc.PageSize {
			pages := int(math.Ceil(float64(len(a.OptionIDs)) / float64(sc.PageSize)))
			if res.Pages < pages {
				// Remember the attribute with the most number option pages
				res.Pages = pages
			}
		}

		for i := offset; i < len(a.OptionIDs); i++ {
			o := db.Option(a.OptionIDs[i])

			if filter != nil {
				if filter.AttributeID == a.ID {
					// Filter matches current attribute, proceed with filtering on `Title`
					if filter.Prefix != "" {
						if !strings.HasPrefix(o.Title, filter.Prefix) {
							// Filtered out!
							continue
						}
					}
				}
			}

			va.Os = append(va.Os, &VisibleOption{ID: o.ID, Title: o.Title})
			visibleOs[o.ID] = true

			count++
			if count >= sc.PageSize {
				break
			}
		}

		if len(va.Os) > 0 {
			// Must have at least one visible option to be considered valid
			res.VAs = append(res.VAs, va)
			visibleAs[va.ID] = va
		}
	}

	// Add options (and their parent attributes) that meet preconditions,
	// i.e. make additional options visible based on what attributes/options
	// are currently selected in our search condition.
	//
	toAdd := make(map[int][]int) // key = attribute ID

	for selectedOptionID := range selectedOs {
		if limitedOptions, found := rule.ShowIfOptionIDSelected[selectedOptionID]; found {
			for _, lo := range limitedOptions {
				if optionIDs, found := toAdd[lo.AttributeID]; found {
					// replace options if this set is smaller!
					if len(lo.OptionIDs) < len(optionIDs) {
						toAdd[lo.AttributeID] = lo.OptionIDs
					}
				} else {
					toAdd[lo.AttributeID] = lo.OptionIDs
				}
			}
		}
	}

	for attributeID, optionIDs := range toAdd {
		a := db.Attribute(attributeID)

		va := &VisibleAttribute{
			ID:    a.ID,
			Title: a.Title,
		}

		res.VAs = append(res.VAs, va)
		visibleAs[a.ID] = va

		for _, optionID := range optionIDs {
			o := db.Option(optionID)

			va.Os = append(va.Os, &VisibleOption{ID: o.ID, Title: o.Title})
			visibleOs[o.ID] = true
		}
	}

	// Clean out invalid attribute conditions
	for _, sac := range sc.Attributes {
		isValidA := visibleAs[sac.AttributeID]
		isValidO := visibleOs[sac.OptionID]
		if isValidA != nil && isValidO {
			res.Corrected.Attributes = append(res.Corrected.Attributes, sac)
		}
	}

	return
}

type FindVisibleAttributesResponse struct {
	Pages     int
	PageSize  int
	VAs       []*VisibleAttribute
	Corrected *SearchConditions
}

type SearchConditions struct {
	CategoryIDs []int                 `json:"category_id"`
	Attributes  []*AttributeCondition `json:"attributes"`
	PageSize    int                   `json:"page_size"`
	Offset      int                   `json:"offset"`
	Filters     []*OptionFilter       `json:"filters"`
}

type OptionFilter struct {
	AttributeID int
	Prefix      string
}

type AttributeCondition struct {
	AttributeID int `json:"attribute_id"`
	OptionID    int `json:"option_id"`
}

type VisibleAttribute struct {
	ID    int              `json:"id"`
	Title string           `json:"title"`
	Os    []*VisibleOption `json:"options"`
}

type VisibleOption struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}
