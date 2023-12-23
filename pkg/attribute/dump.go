package attribute

import (
	"fmt"
	"sort"
)

type DumpOpts struct {
	OnlyCategoryID  int
	MaxLinesToPrint int
}

func (db *DB) Dump(o DumpOpts) {
	if o.MaxLinesToPrint == 0 {
		// Print max 5 attributes, options and dynamic options per node
		// in the tree by default
		o.MaxLinesToPrint = 5
	}

	// Print all category rules
	{
		if o.OnlyCategoryID > 0 {
			cr := db.CategoryRules[o.OnlyCategoryID]
			db.DumpCategoryRule(cr, o.MaxLinesToPrint)
		} else {
			var categoryIDs []int
			for id := range db.CategoryRules {
				categoryIDs = append(categoryIDs, id)
			}
			sort.Slice(categoryIDs, func(i, j int) bool {
				return categoryIDs[i] < categoryIDs[j]
			})

			for _, id := range categoryIDs {
				cr := db.CategoryRules[id]
				db.DumpCategoryRule(cr, o.MaxLinesToPrint)
			}
		}
	}

	fmt.Println("")

	fmt.Println("=== Stats ===")
	fmt.Printf("Attributes        : %d\n", len(db.Attributes))
	fmt.Printf("Options           : %d\n", len(db.Options))
	fmt.Printf("Category Rules    : %d\n", len(db.CategoryRules))

	// Count the approx. number of references present in the DB
	var refs int
	for _, a := range db.Attributes {
		refs += 1 + len(a.OptionIDs)
	}
	refs += len(db.Options)
	refs += len(db.IDs)
	refs += len(db.ReverseIDs)
	for _, cr := range db.CategoryRules {
		refs += 1 + len(cr.AttributeIDs) + len(cr.AlwaysVisibleAttributeIDs)
		for _, los := range cr.ShowIfOptionIDSelected {
			refs += 1
			for _, lo := range los {
				refs += 1 + len(lo.OptionIDs)
			}
		}
	}
	fmt.Printf("Refs              : %d\n\n", refs)
}

func (db *DB) DumpCategoryRule(cr *CategoryRule, max int) {
	fmt.Printf(
		"category [%6d] - %s (%d / %d attributes, %d conditions)\n",
		cr.CategoryID, db.FullCategoryName(cr.CategoryID),
		len(cr.AlwaysVisibleAttributeIDs), len(cr.AttributeIDs), len(cr.ShowIfOptionIDSelected),
	)

	for _, attributeID := range cr.AlwaysVisibleAttributeIDs {
		a := db.Attribute(attributeID)

		fmt.Printf(" - attribute [%6d] - %s (%d)\n", a.ID, a.Title, a.DisplayOrder)
		for i, optionID := range a.OptionIDs {
			o := db.Option(optionID)

			fmt.Printf("    - option [%6d] - %s (%d)\n", o.ID, o.Title, o.DisplayOrder)
			if i >= 50 {
				fmt.Printf("      <skipped %d more>\n", len(a.OptionIDs)-i)
				break
			}
		}
	}

	for selectedOptionID, limitedOptions := range cr.ShowIfOptionIDSelected {
		so := db.Option(selectedOptionID)
		soa := db.Attribute(so.AttributeID)

		fmt.Printf(" - precondition: attribute [%6d] - %s - option [%6d] - %s\n", soa.ID, soa.Title, so.ID, so.Title)

		for _, lo := range limitedOptions {
			a := db.Attribute(lo.AttributeID)
			fmt.Printf("    - attribute [%6d] - %s (%d)\n", a.ID, a.Title, a.DisplayOrder)
			for _, optionID := range lo.OptionIDs {
				o := db.Option(optionID)
				fmt.Printf("       - option [%6d] - %s (%d)\n", o.ID, o.Title, o.DisplayOrder)
			}
		}
	}
}
