package attribute

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAttributeDB(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Attributes DB Suite")
}

var db *DB

var _ = BeforeSuite(func() {
	db = NewDB()

	err := db.LoadCategoriesJSON("../../../test-data/categories.json")
	Expect(err).ToNot(HaveOccurred())

	err = db.ImportPostgresDatabase(ImportPostgresDatabaseArgs{Dir: "../../../test-data"})
	Expect(err).ToNot(HaveOccurred())

	db.PreSort()
})

var _ = Describe("Importing attributes database", Label("attributes"), func() {
	// category 242 - レディース - 小物 - 折り財布 (15 attributes)
	categoryID := 242

	When("the attributes DB is loaded", func() {
		It("category 242 should have a rule", func() {
			Expect(db.FullCategoryName(categoryID)).To(Equal("レディース - 小物 - 折り財布"))
			rule := db.CategoryRules[categoryID]
			Expect(rule.AttributeIDs).To(HaveLen(15))
			Expect(rule.AlwaysVisibleAttributeIDs).To(HaveLen(5))
		})

		// find := func(id int, vas []*VisibleAttribute) *VisibleAttribute {
		// 	for _, va := range vas {
		// 		if va.ID == id {
		// 			return va
		// 		}
		// 	}
		// 	return nil
		// }

		It("should return all visible attributes when category 242 is selected", func() {
			res, err := FindVisibleAttributes(&SearchConditions{
				CategoryIDs: []int{categoryID},
				PageSize:    10,
			}, db)

			Expect(err).ToNot(HaveOccurred())
			Expect(res.VAs).To(HaveLen(5))

			rule := db.CategoryRules[categoryID]

			Expect(res.VAs).To(HaveLen(len(rule.AlwaysVisibleAttributeIDs)))
			Expect(res.VAs[0].ID).To(Equal(rule.AlwaysVisibleAttributeIDs[0]))
			Expect(res.Pages).To(BeNumerically(">=", 500)) // Expect lots of pages since brands are returned!
			Expect(len(res.VAs)).To(BeNumerically("<", len(rule.AttributeIDs)))
		})

		It("should limit options for attribute when conditions are met", func() {
			baseCase, err := FindVisibleAttributes(&SearchConditions{
				CategoryIDs: []int{categoryID},
			}, db)
			Expect(err).ToNot(HaveOccurred())
			Expect(baseCase.VAs).To(HaveLen(5))

			rule := db.CategoryRules[categoryID]

			var requiredOptionID int
			for id := range rule.ShowIfOptionIDSelected {
				requiredOptionID = id
				break
			}

			requiredOption, found := db.Options[requiredOptionID]
			Expect(found).To(BeTrue())

			res, err := FindVisibleAttributes(&SearchConditions{
				CategoryIDs: []int{categoryID},
				Attributes: []*AttributeCondition{
					{AttributeID: requiredOption.AttributeID, OptionID: requiredOption.ID},
				},
			}, db)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(res.VAs)).To(BeNumerically(">", len(baseCase.VAs)))
		})
	})
})
