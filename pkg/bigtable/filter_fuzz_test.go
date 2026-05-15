package bigtable

import (
	"regexp"
	"testing"

	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
)

func FuzzBuildEvaluator(f *testing.F) {
	// Seed with various filter configurations.
	f.Add([]byte("^row.*$"), "cf1", []byte("^col.*$"), int64(100), int64(200), int32(5), int32(10), int32(3))
	f.Add([]byte(""), "", []byte(""), int64(0), int64(0), int32(0), int32(0), int32(0))

	f.Fuzz(func(t *testing.T, rowRegex []byte, family string, qualRegex []byte, startTS, endTS int64, offset, limit, colLimit int32) {
		// Build various filter types and verify they don't panic.
		filters := []*bigtablepb.RowFilter{
			{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
			{Filter: &bigtablepb.RowFilter_BlockAllFilter{}},
		}

		if len(rowRegex) > 0 {
			filters = append(filters, &bigtablepb.RowFilter{
				Filter: &bigtablepb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: rowRegex},
			})
		}
		if family != "" {
			filters = append(filters, &bigtablepb.RowFilter{
				Filter: &bigtablepb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: family},
			})
		}
		if len(qualRegex) > 0 {
			filters = append(filters, &bigtablepb.RowFilter{
				Filter: &bigtablepb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: qualRegex},
			})
		}

		filters = append(filters, &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_TimestampRangeFilter{
				TimestampRangeFilter: &bigtablepb.TimestampRange{
					StartTimestampMicros: startTS,
					EndTimestampMicros:   endTS,
				},
			},
		})

		filters = append(filters,
			&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_CellsPerRowOffsetFilter{CellsPerRowOffsetFilter: offset}},
			&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_CellsPerRowLimitFilter{CellsPerRowLimitFilter: limit}},
			&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_CellsPerColumnLimitFilter{CellsPerColumnLimitFilter: colLimit}},
			&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_StripValueTransformer{}},
			&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_ApplyLabelTransformer{ApplyLabelTransformer: "label"}},
		)

		// Column range filter.
		filters = append(filters, &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_ColumnRangeFilter{
				ColumnRangeFilter: &bigtablepb.ColumnRange{
					FamilyName: "cf",
					StartQualifier: &bigtablepb.ColumnRange_StartQualifierClosed{
						StartQualifierClosed: []byte("a"),
					},
					EndQualifier: &bigtablepb.ColumnRange_EndQualifierOpen{
						EndQualifierOpen: []byte("z"),
					},
				},
			},
		})

		// Test each simple filter.
		for _, filter := range filters {
			eval, err := buildEvaluator(filter)
			if err != nil {
				// Invalid regex is expected to error, that's fine.
				continue
			}
			// Should not panic on evaluate/reset.
			eval.evaluate(cellInfo{
				rowKey:    []byte("testrow"),
				family:    "cf",
				qualifier: []byte("col"),
				ts:        150,
				value:     []byte("value"),
			})
			eval.reset()
		}

		// Test chain of filters.
		chain := &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_Chain_{
				Chain: &bigtablepb.RowFilter_Chain{Filters: filters[:2]},
			},
		}
		eval, err := buildEvaluator(chain)
		if err == nil {
			eval.evaluate(cellInfo{rowKey: []byte("row"), family: "cf", qualifier: []byte("q"), ts: 100})
			eval.reset()
		}

		// Test interleave of filters.
		interleave := &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_Interleave_{
				Interleave: &bigtablepb.RowFilter_Interleave{Filters: filters[:2]},
			},
		}
		eval, err = buildEvaluator(interleave)
		if err == nil {
			eval.evaluate(cellInfo{rowKey: []byte("row"), family: "cf", qualifier: []byte("q"), ts: 100})
			eval.reset()
		}

		// Test condition filter.
		cond := &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_Condition_{
				Condition: &bigtablepb.RowFilter_Condition{
					PredicateFilter: filters[0],
					TrueFilter:      filters[1],
					FalseFilter:     filters[2],
				},
			},
		}
		eval, err = buildEvaluator(cond)
		if err == nil {
			eval.evaluate(cellInfo{rowKey: []byte("row"), family: "cf", qualifier: []byte("q"), ts: 100})
			eval.reset()
		}
	})
}

func FuzzRowFilterEngine(f *testing.F) {
	f.Add([]byte("^row.*$"), int64(100), int64(200))

	f.Fuzz(func(t *testing.T, regex []byte, startTS, endTS int64) {
		// Build filters from arbitrary input and verify no panics.
		filterConfigs := []*bigtablepb.RowFilter{
			nil,
			{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
			{Filter: &bigtablepb.RowFilter_BlockAllFilter{}},
			{Filter: &bigtablepb.RowFilter_StripValueTransformer{}},
			{Filter: &bigtablepb.RowFilter_ApplyLabelTransformer{ApplyLabelTransformer: "test"}},
			{Filter: &bigtablepb.RowFilter_CellsPerRowOffsetFilter{CellsPerRowOffsetFilter: 5}},
			{Filter: &bigtablepb.RowFilter_CellsPerRowLimitFilter{CellsPerRowLimitFilter: 10}},
			{Filter: &bigtablepb.RowFilter_CellsPerColumnLimitFilter{CellsPerColumnLimitFilter: 3}},
		}

		if len(regex) > 0 {
			// Only add regex filters if they compile.
			if _, err := regexp.Compile(string(regex)); err == nil {
				filterConfigs = append(filterConfigs,
					&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: regex}},
					&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: regex}},
				)
			}
		}

		filterConfigs = append(filterConfigs,
			&bigtablepb.RowFilter{
				Filter: &bigtablepb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "cf"},
			},
			&bigtablepb.RowFilter{
				Filter: &bigtablepb.RowFilter_TimestampRangeFilter{
					TimestampRangeFilter: &bigtablepb.TimestampRange{
						StartTimestampMicros: startTS,
						EndTimestampMicros:   endTS,
					},
				},
			},
		)

		for _, fc := range filterConfigs {
			engine, err := newRowFilterEngine(fc)
			if err != nil {
				continue
			}
			// Test matchesCell with various inputs.
			engine.matchesCell([]byte("row"), "cf", []byte("q"), 150, []byte("v"))
			engine.matchesCell([]byte{}, "", []byte{}, 0, nil)
			engine.matchesCell([]byte{0x00, 0xFF}, "fam", []byte{0xFF, 0x00}, -1, []byte{})
		}
	})
}

func FuzzColumnRangeFilter(f *testing.F) {
	f.Add([]byte("a"), []byte("z"), true, true)
	f.Add([]byte("m"), []byte("m"), true, false)
	f.Add([]byte(""), []byte(""), false, false)

	f.Fuzz(func(t *testing.T, startQ, endQ []byte, startIncl, endIncl bool) {
		if len(startQ) > 1024 {
			startQ = startQ[:1024]
		}
		if len(endQ) > 1024 {
			endQ = endQ[:1024]
		}

		f := &columnRangeFilter{
			family:         "cf",
			startQualifier: startQ,
			endQualifier:   endQ,
			startInclusive: startIncl,
			endInclusive:   endIncl,
			rangeSet:       len(startQ) > 0 || len(endQ) > 0,
		}

		// Test with various qualifiers.
		testCases := [][]byte{
			{},
			[]byte("a"),
			[]byte("m"),
			[]byte("z"),
			[]byte("zzz"),
			startQ,
			endQ,
		}

		for _, qual := range testCases {
			// Should not panic.
			f.evaluate(cellInfo{family: "cf", qualifier: qual, ts: 100})
		}
		f.reset()
	})
}

func FuzzConditionFilterState(f *testing.F) {
	f.Add(true, true, false)
	f.Add(false, false, true)

	f.Fuzz(func(t *testing.T, predPass, truePass, falsePass bool) {
		var pred filterEvaluator = &passAllFilter{}
		if !predPass {
			pred = &blockAllFilter{}
		}
		var tf filterEvaluator = &passAllFilter{}
		if !truePass {
			tf = &blockAllFilter{}
		}
		var ff filterEvaluator = &passAllFilter{}
		if !falsePass {
			ff = &blockAllFilter{}
		}

		cond := &conditionFilter{
			predicate:   pred,
			trueFilter:  tf,
			falseFilter: ff,
		}

		// Evaluate multiple times - should be consistent (stateful).
		result1 := cond.evaluate(cellInfo{ts: 1})
		result2 := cond.evaluate(cellInfo{ts: 2})
		if result1 != result2 {
			t.Errorf("condition filter should be stateful within a row: got %v then %v", result1, result2)
		}

		// After reset, should re-evaluate.
		cond.reset()
		_ = cond.evaluate(cellInfo{ts: 3})
	})
}

func FuzzInterleaveFilterDedup(f *testing.F) {
	f.Add("cf1", "cf2", "cf3")

	f.Fuzz(func(t *testing.T, fam1, fam2, fam3 string) {
		if len(fam1) > 64 {
			fam1 = fam1[:64]
		}
		if len(fam2) > 64 {
			fam2 = fam2[:64]
		}
		if len(fam3) > 64 {
			fam3 = fam3[:64]
		}
		_ = fam3
		if fam1 == "" {
			fam1 = "f1"
		}
		if fam2 == "" {
			fam2 = "f2"
		}

		re1, err := regexp.Compile("^" + regexp.QuoteMeta(fam1) + "$")
		if err != nil {
			return
		}
		re2, err := regexp.Compile("^" + regexp.QuoteMeta(fam2) + "$")
		if err != nil {
			return
		}

		il := &interleaveFilter{
			filters: []filterEvaluator{
				&familyRegexFilter{re: re1},
				&familyRegexFilter{re: re2},
			},
		}

		// Same cell should pass once, then be deduped.
		cell := cellInfo{family: fam1, qualifier: []byte("q"), ts: 100}
		passed := 0
		if il.evaluate(cell) {
			passed++
		}
		if il.evaluate(cell) {
			passed++
		}
		if passed > 1 {
			t.Errorf("interleave should deduplicate: passed %d times", passed)
		}

		// After reset, should pass again.
		il.reset()
		if !il.evaluate(cell) {
			t.Errorf("after reset, cell should pass again")
		}
	})
}
