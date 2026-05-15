package bigtable

import (
	"bytes"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
)

// setupTestTable creates a server with a table and populates rows for filter testing.
// Row layout:
//
//	row1: cf1:a@t1=v1, cf1:a@t2=v2, cf2:b@t1=v3
//	row2: cf1:a@t1=v4
func setupTestTable(t *testing.T) (*Server, string) {
	t.Helper()
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/filtertest"
	eng := openTableEngine(t, s, table)
	db := eng.DB()

	now := time.Now()
	write := func(row, fam, qual string, ts int64, val string) {
		key := EncodeCellKey([]byte(row), fam, []byte(qual), ts)
		if err := db.Set(key, []byte(val), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}

	write("row1", "cf1", "a", now.UnixMicro(), "v1")
	write("row1", "cf1", "a", now.UnixMicro()-1000, "v2")
	write("row1", "cf2", "b", now.UnixMicro(), "v3")
	write("row2", "cf1", "a", now.UnixMicro(), "v4")

	return s, table
}

// filterTestIter creates a pebble iterator over all data in the test table.
func filterTestIter(t *testing.T, s *Server, table string) *pebble.Iterator {
	t.Helper()
	eng := openTableEngine(t, s, table)
	db := eng.DB()
	iter, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return iter
}

func TestPassAllFilter(t *testing.T) {
	f := &passAllFilter{}
	if !f.evaluate(cellInfo{}) {
		t.Fatal("passAll should always return true")
	}
	f.reset() // should not panic
}

func TestBlockAllFilter(t *testing.T) {
	f := &blockAllFilter{}
	if f.evaluate(cellInfo{}) {
		t.Fatal("blockAll should always return false")
	}
}

func TestRowKeyRegexFilter(t *testing.T) {
	f := &rowKeyRegexFilter{re: regexp.MustCompile("^row1$")}
	if !f.evaluate(cellInfo{rowKey: []byte("row1")}) {
		t.Fatal("should match row1")
	}
	if f.evaluate(cellInfo{rowKey: []byte("row2")}) {
		t.Fatal("should not match row2")
	}
}

func TestFamilyRegexFilter(t *testing.T) {
	f := &familyRegexFilter{re: regexp.MustCompile("^cf[12]$")}
	if !f.evaluate(cellInfo{family: "cf1"}) {
		t.Fatal("should match cf1")
	}
	if !f.evaluate(cellInfo{family: "cf2"}) {
		t.Fatal("should match cf2")
	}
	if f.evaluate(cellInfo{family: "cf3"}) {
		t.Fatal("should not match cf3")
	}
}

func TestQualifierRegexFilter(t *testing.T) {
	f := &qualifierRegexFilter{re: regexp.MustCompile("^[ab]$")}
	if !f.evaluate(cellInfo{qualifier: []byte("a")}) {
		t.Fatal("should match a")
	}
	if !f.evaluate(cellInfo{qualifier: []byte("b")}) {
		t.Fatal("should match b")
	}
	if f.evaluate(cellInfo{qualifier: []byte("c")}) {
		t.Fatal("should not match c")
	}
}

func TestColumnRangeFilterFamilyOnly(t *testing.T) {
	f := &columnRangeFilter{family: "cf1"}
	if !f.evaluate(cellInfo{family: "cf1", qualifier: []byte("a")}) {
		t.Fatal("should match cf1")
	}
	if f.evaluate(cellInfo{family: "cf2", qualifier: []byte("a")}) {
		t.Fatal("should not match cf2")
	}
}

func TestColumnRangeFilterClosedClosed(t *testing.T) {
	f := &columnRangeFilter{
		family:         "cf1",
		startQualifier: []byte("a"),
		endQualifier:   []byte("c"),
		startInclusive: true,
		endInclusive:   true,
		rangeSet:       true,
	}
	if !f.evaluate(cellInfo{family: "cf1", qualifier: []byte("a")}) {
		t.Fatal("should include a")
	}
	if !f.evaluate(cellInfo{family: "cf1", qualifier: []byte("b")}) {
		t.Fatal("should include b")
	}
	if !f.evaluate(cellInfo{family: "cf1", qualifier: []byte("c")}) {
		t.Fatal("should include c")
	}
	if f.evaluate(cellInfo{family: "cf1", qualifier: []byte("d")}) {
		t.Fatal("should exclude d")
	}
}

func TestColumnRangeFilterOpenOpen(t *testing.T) {
	f := &columnRangeFilter{
		family:         "cf1",
		startQualifier: []byte("a"),
		endQualifier:   []byte("c"),
		startInclusive: false,
		endInclusive:   false,
		rangeSet:       true,
	}
	if f.evaluate(cellInfo{family: "cf1", qualifier: []byte("a")}) {
		t.Fatal("should exclude a (open start)")
	}
	if !f.evaluate(cellInfo{family: "cf1", qualifier: []byte("b")}) {
		t.Fatal("should include b")
	}
	if f.evaluate(cellInfo{family: "cf1", qualifier: []byte("c")}) {
		t.Fatal("should exclude c (open end)")
	}
}

func TestColumnRangeFilterNoRangeSet(t *testing.T) {
	f := &columnRangeFilter{family: "cf1"}
	if !f.evaluate(cellInfo{family: "cf1", qualifier: []byte("anything")}) {
		t.Fatal("no range set should pass all within family")
	}
}

func TestTimestampRangeFilter(t *testing.T) {
	f := &timestampRangeFilter{startMicros: 100, endMicros: 200}
	if f.evaluate(cellInfo{ts: 50}) {
		t.Fatal("should exclude ts=50 (before start)")
	}
	if !f.evaluate(cellInfo{ts: 150}) {
		t.Fatal("should include ts=150 (within range)")
	}
	if f.evaluate(cellInfo{ts: 200}) {
		t.Fatal("should exclude ts=200 (end exclusive)")
	}
	if f.evaluate(cellInfo{ts: 250}) {
		t.Fatal("should exclude ts=250 (after end)")
	}
}

func TestTimestampRangeFilterNoStart(t *testing.T) {
	f := &timestampRangeFilter{endMicros: 200}
	if !f.evaluate(cellInfo{ts: 50}) {
		t.Fatal("no start should pass ts=50")
	}
	if f.evaluate(cellInfo{ts: 200}) {
		t.Fatal("should exclude ts=200 (end exclusive)")
	}
}

func TestTimestampRangeFilterNoEnd(t *testing.T) {
	f := &timestampRangeFilter{startMicros: 100}
	if f.evaluate(cellInfo{ts: 50}) {
		t.Fatal("should exclude ts=50 (before start)")
	}
	if !f.evaluate(cellInfo{ts: 200}) {
		t.Fatal("no end should pass ts=200")
	}
}

func TestCellsPerRowOffsetFilter(t *testing.T) {
	f := &cellsPerRowOffsetFilter{offset: 2}
	if f.evaluate(cellInfo{}) {
		t.Fatal("first cell should be skipped")
	}
	if f.evaluate(cellInfo{}) {
		t.Fatal("second cell should be skipped")
	}
	if !f.evaluate(cellInfo{}) {
		t.Fatal("third cell should pass")
	}
	f.reset()
	if f.evaluate(cellInfo{}) {
		t.Fatal("after reset, first cell should be skipped again")
	}
}

func TestCellsPerRowLimitFilter(t *testing.T) {
	f := &cellsPerRowLimitFilter{limit: 2}
	if !f.evaluate(cellInfo{}) {
		t.Fatal("first cell should pass")
	}
	if !f.evaluate(cellInfo{}) {
		t.Fatal("second cell should pass")
	}
	if f.evaluate(cellInfo{}) {
		t.Fatal("third cell should be blocked")
	}
	f.reset()
	if !f.evaluate(cellInfo{}) {
		t.Fatal("after reset, first cell should pass")
	}
}

func TestCellsPerColumnLimitFilter(t *testing.T) {
	f := &cellsPerColumnLimitFilter{limit: 2}
	if !f.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 1}) {
		t.Fatal("first cell in cf:q should pass")
	}
	if !f.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 2}) {
		t.Fatal("second cell in cf:q should pass")
	}
	if f.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 3}) {
		t.Fatal("third cell in cf:q should be blocked")
	}
	// Different column should be unaffected.
	if !f.evaluate(cellInfo{family: "cf2", qualifier: []byte("q2"), ts: 1}) {
		t.Fatal("first cell in cf2:q2 should pass")
	}
	f.reset()
	if !f.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 4}) {
		t.Fatal("after reset, first cell in cf:q should pass")
	}
}

func TestStripValueFilter(t *testing.T) {
	f := &stripValueFilter{}
	if !f.evaluate(cellInfo{value: []byte("data")}) {
		t.Fatal("strip value should pass all cells")
	}
	f.reset() // should not panic
}

func TestApplyLabelFilter(t *testing.T) {
	f := &applyLabelFilter{label: "test-label"}
	if !f.evaluate(cellInfo{}) {
		t.Fatal("apply label should pass all cells")
	}
	f.reset() // should not panic
}

func TestChainFilter(t *testing.T) {
	re := regexp.MustCompile("^row1$")
	chain := &chainFilter{
		filters: []filterEvaluator{
			&rowKeyRegexFilter{re: re},
			&blockAllFilter{},
		},
	}
	if chain.evaluate(cellInfo{rowKey: []byte("row1")}) {
		t.Fatal("chain with blockAll should always return false")
	}
	chain.reset() // should not panic

	chain2 := &chainFilter{
		filters: []filterEvaluator{
			&rowKeyRegexFilter{re: re},
			&passAllFilter{},
		},
	}
	if !chain2.evaluate(cellInfo{rowKey: []byte("row1")}) {
		t.Fatal("chain pass through: should pass row1")
	}
	if chain2.evaluate(cellInfo{rowKey: []byte("row2")}) {
		t.Fatal("chain pass through: should not pass row2")
	}
}

func TestInterleaveFilter(t *testing.T) {
	il := &interleaveFilter{
		filters: []filterEvaluator{
			&familyRegexFilter{re: regexp.MustCompile("^cf1$")},
			&familyRegexFilter{re: regexp.MustCompile("^cf2$")},
		},
	}
	if !il.evaluate(cellInfo{family: "cf1", qualifier: []byte("a"), ts: 1}) {
		t.Fatal("interleave should pass cf1:1")
	}
	if !il.evaluate(cellInfo{family: "cf2", qualifier: []byte("b"), ts: 2}) {
		t.Fatal("interleave should pass cf2:2")
	}
	if il.evaluate(cellInfo{family: "cf3", qualifier: []byte("c"), ts: 3}) {
		t.Fatal("interleave should not pass cf3")
	}

	il2 := &interleaveFilter{
		filters: []filterEvaluator{
			&passAllFilter{},
			&passAllFilter{},
		},
	}
	if !il2.evaluate(cellInfo{family: "cf1", qualifier: []byte("a"), ts: 1}) {
		t.Fatal("interleave with two passAll should pass once")
	}
	// Same cell should be deduplicated.
	if il2.evaluate(cellInfo{family: "cf1", qualifier: []byte("a"), ts: 1}) {
		t.Fatal("interleave should dedup same cell")
	}
	il2.reset()
	if !il2.evaluate(cellInfo{family: "cf1", qualifier: []byte("a"), ts: 1}) {
		t.Fatal("after reset, cell should pass again")
	}
}

func TestConditionFilterPredicateMatched(t *testing.T) {
	cond := &conditionFilter{
		predicate:   &passAllFilter{},
		trueFilter:  &blockAllFilter{},
		falseFilter: &passAllFilter{},
	}
	// Predicate passes, so true filter (blockAll) is used.
	if cond.evaluate(cellInfo{}) {
		t.Fatal("condition with passAll predicate + blockAll true should block")
	}
}

func TestConditionFilterPredicateNotMatched(t *testing.T) {
	cond := &conditionFilter{
		predicate:   &blockAllFilter{},
		trueFilter:  &blockAllFilter{},
		falseFilter: &passAllFilter{},
	}
	// Predicate fails, so false filter (passAll) is used.
	if !cond.evaluate(cellInfo{}) {
		t.Fatal("condition with blockAll predicate + passAll false should pass")
	}
}

func TestConditionFilterNoTrueFilter(t *testing.T) {
	cond := &conditionFilter{
		predicate:  &passAllFilter{},
		falseFilter: &passAllFilter{},
	}
	// Predicate passes, but true filter is nil → return false.
	if cond.evaluate(cellInfo{}) {
		t.Fatal("condition with no true filter should block when predicate matched")
	}
}

func TestConditionFilterNoFalseFilter(t *testing.T) {
	cond := &conditionFilter{
		predicate:  &blockAllFilter{},
		trueFilter: &passAllFilter{},
	}
	// Predicate fails, false filter is nil → return false.
	if cond.evaluate(cellInfo{}) {
		t.Fatal("condition with no false filter should block when predicate not matched")
	}
}

func TestConditionFilterReset(t *testing.T) {
	cond := &conditionFilter{
		predicate:   &passAllFilter{},
		trueFilter:  &passAllFilter{},
		falseFilter: &blockAllFilter{},
	}
	cond.evaluate(cellInfo{})
	cond.reset()
	if !cond.evaluate(cellInfo{}) {
		t.Fatal("after reset, condition should re-evaluate predicate")
	}
}

func TestBuildEvaluatorPassAll(t *testing.T) {
	e, err := buildEvaluator(&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_PassAllFilter{}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("passAll should pass")
	}
}

func TestBuildEvaluatorBlockAll(t *testing.T) {
	e, err := buildEvaluator(&bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_BlockAllFilter{}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.evaluate(cellInfo{}) {
		t.Fatal("blockAll should block")
	}
}

func TestBuildEvaluatorNil(t *testing.T) {
	e, err := buildEvaluator(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("nil filter should pass all")
	}
}

func TestBuildEvaluatorChain(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_Chain_{
			Chain: &bigtablepb.RowFilter_Chain{
				Filters: []*bigtablepb.RowFilter{
					{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
					{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
				},
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("chain of passAll should pass")
	}
}

func TestBuildEvaluatorInterleave(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_Interleave_{
			Interleave: &bigtablepb.RowFilter_Interleave{
				Filters: []*bigtablepb.RowFilter{
					{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
					{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
				},
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("interleave of passAll should pass")
	}
}

func TestBuildEvaluatorCondition(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_Condition_{
			Condition: &bigtablepb.RowFilter_Condition{
				PredicateFilter: &bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
				TrueFilter:      &bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
				FalseFilter:     &bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_BlockAllFilter{}},
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("condition should apply true filter")
	}
}

func TestBuildEvaluatorRowKeyRegex(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_RowKeyRegexFilter{
			RowKeyRegexFilter: []byte("^row1$"),
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{rowKey: []byte("row1")}) {
		t.Fatal("should match row1")
	}
	if e.evaluate(cellInfo{rowKey: []byte("row2")}) {
		t.Fatal("should not match row2")
	}
}

func TestBuildEvaluatorFamilyNameRegex(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_FamilyNameRegexFilter{
			FamilyNameRegexFilter: "cf1",
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{family: "cf1"}) {
		t.Fatal("should match cf1")
	}
}

func TestBuildEvaluatorColumnQualifierRegex(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_ColumnQualifierRegexFilter{
			ColumnQualifierRegexFilter: []byte("^a$"),
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{qualifier: []byte("a")}) {
		t.Fatal("should match a")
	}
}

func TestBuildEvaluatorColumnRange(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_ColumnRangeFilter{
			ColumnRangeFilter: &bigtablepb.ColumnRange{
				FamilyName: "cf",
				StartQualifier: &bigtablepb.ColumnRange_StartQualifierClosed{
					StartQualifierClosed: []byte("a"),
				},
				EndQualifier: &bigtablepb.ColumnRange_EndQualifierOpen{
					EndQualifierOpen: []byte("c"),
				},
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{family: "cf", qualifier: []byte("a")}) {
		t.Fatal("should include a (closed start)")
	}
	if !e.evaluate(cellInfo{family: "cf", qualifier: []byte("b")}) {
		t.Fatal("should include b")
	}
	if e.evaluate(cellInfo{family: "cf", qualifier: []byte("c")}) {
		t.Fatal("should exclude c (open end)")
	}
}

func TestBuildEvaluatorTimestampRange(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_TimestampRangeFilter{
			TimestampRangeFilter: &bigtablepb.TimestampRange{
				StartTimestampMicros: 100,
				EndTimestampMicros:   200,
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.evaluate(cellInfo{ts: 50}) {
		t.Fatal("should exclude before start")
	}
	if !e.evaluate(cellInfo{ts: 150}) {
		t.Fatal("should include within range")
	}
}

func TestBuildEvaluatorCellsPerRowOffset(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_CellsPerRowOffsetFilter{
			CellsPerRowOffsetFilter: 2,
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.evaluate(cellInfo{}) {
		t.Fatal("first cell should be offset")
	}
	if e.evaluate(cellInfo{}) {
		t.Fatal("second cell should be offset")
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("third cell should pass")
	}
}

func TestBuildEvaluatorCellsPerRowLimit(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_CellsPerRowLimitFilter{
			CellsPerRowLimitFilter: 2,
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("first cell should pass")
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("second cell should pass")
	}
	if e.evaluate(cellInfo{}) {
		t.Fatal("third cell should be limited")
	}
}

func TestBuildEvaluatorCellsPerColumnLimit(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_CellsPerColumnLimitFilter{
			CellsPerColumnLimitFilter: 1,
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 1}) {
		t.Fatal("first cell should pass")
	}
	if e.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 2}) {
		t.Fatal("second cell should be limited")
	}
}

func TestBuildEvaluatorStripValue(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_StripValueTransformer{},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{value: []byte("data")}) {
		t.Fatal("strip value should pass")
	}
}

func TestBuildEvaluatorApplyLabel(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_ApplyLabelTransformer{
			ApplyLabelTransformer: "mylabel",
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("apply label should pass")
	}
}

func TestBuildEvaluatorUnsupported(t *testing.T) {
	// Unsupported filters should act as passAll.
	e, err := buildEvaluator(&bigtablepb.RowFilter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("unsupported filter should pass all")
	}
}

func TestBuildEvaluatorChainNested(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_Chain_{
			Chain: &bigtablepb.RowFilter_Chain{
				Filters: []*bigtablepb.RowFilter{
					{
						Filter: &bigtablepb.RowFilter_Chain_{
							Chain: &bigtablepb.RowFilter_Chain{
								Filters: []*bigtablepb.RowFilter{
									{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
								},
							},
						},
					},
				},
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{}) {
		t.Fatal("nested chain should pass")
	}
}

func TestFilterEngineProcess(t *testing.T) {
	s, table := setupTestTable(t)
	iter := filterTestIter(t, s, table)
	defer iter.Close()

	engine, err := newRowFilterEngine(nil)
	if err != nil {
		t.Fatal(err)
	}

	var cells []cellInfo
	rp := &RowProcessor{
		OnCell: func(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) bool {
			cells = append(cells, cellInfo{rowKey: rowKey, family: family, qualifier: qualifier, ts: timestampMicros, value: value})
			return true
		},
	}
	engine.process(iter, rp)

	if len(cells) != 4 {
		t.Fatalf("expected 4 cells, got %d", len(cells))
	}
	if !rp.Matched {
		t.Fatal("expected matched to be true")
	}
}

func TestFilterEngineProcessWithBlockAll(t *testing.T) {
	s, table := setupTestTable(t)
	iter := filterTestIter(t, s, table)
	defer iter.Close()

	engine, err := newRowFilterEngine(&bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_BlockAllFilter{},
	})
	if err != nil {
		t.Fatal(err)
	}

	var cells []cellInfo
	rp := &RowProcessor{
		OnCell: func(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) bool {
			cells = append(cells, cellInfo{})
			return true
		},
	}
	engine.process(iter, rp)

	if len(cells) != 0 {
		t.Fatalf("expected 0 cells with blockAll, got %d", len(cells))
	}
	if rp.Matched {
		t.Fatal("expected matched to be false")
	}
}

func TestFilterEngineProcessWithStop(t *testing.T) {
	s, table := setupTestTable(t)
	iter := filterTestIter(t, s, table)
	defer iter.Close()

	engine, err := newRowFilterEngine(nil)
	if err != nil {
		t.Fatal(err)
	}

	callCount := 0
	rp := &RowProcessor{
		OnCell: func(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) bool {
			callCount++
			return false // stop after first
		},
	}
	engine.process(iter, rp)

	if callCount != 1 {
		t.Fatalf("expected 1 callback call, got %d", callCount)
	}
}

func TestFilterEngineProcessStripValue(t *testing.T) {
	s, table := setupTestTable(t)
	iter := filterTestIter(t, s, table)
	defer iter.Close()

	engine, err := newRowFilterEngine(&bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_StripValueTransformer{},
	})
	if err != nil {
		t.Fatal(err)
	}

	var cells []cellInfo
	rp := &RowProcessor{
		OnCell: func(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) bool {
			cells = append(cells, cellInfo{value: value})
			return true
		},
	}
	engine.process(iter, rp)

	if len(cells) != 4 {
		t.Fatalf("expected 4 cells, got %d", len(cells))
	}
	for _, c := range cells {
		if c.value != nil {
			t.Fatal("expected stripped values to be nil")
		}
	}
}

func TestFilterEngineProcessApplyLabel(t *testing.T) {
	s, table := setupTestTable(t)
	iter := filterTestIter(t, s, table)
	defer iter.Close()

	engine, err := newRowFilterEngine(&bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_ApplyLabelTransformer{
			ApplyLabelTransformer: "mylabel",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var gotLabels []string
	rp := &RowProcessor{
		OnCell: func(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) bool {
			gotLabels = labels
			return true
		},
	}
	engine.process(iter, rp)

	if len(gotLabels) != 1 || gotLabels[0] != "mylabel" {
		t.Fatalf("expected label 'mylabel', got %v", gotLabels)
	}
}

func TestFilterEngineProcessRowKeyRegex(t *testing.T) {
	s, table := setupTestTable(t)
	iter := filterTestIter(t, s, table)
	defer iter.Close()

	engine, err := newRowFilterEngine(&bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_RowKeyRegexFilter{
			RowKeyRegexFilter: []byte("^row1$"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var cells []cellInfo
	rp := &RowProcessor{
		OnCell: func(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) bool {
			cells = append(cells, cellInfo{rowKey: rowKey})
			return true
		},
	}
	engine.process(iter, rp)

	if len(cells) != 3 {
		t.Fatalf("expected 3 cells for row1, got %d", len(cells))
	}
	for _, c := range cells {
		if !bytes.Equal(c.rowKey, []byte("row1")) {
			t.Fatalf("expected all cells from row1, got %v", c.rowKey)
		}
	}
}

func TestFilterEngineHasMatch(t *testing.T) {
	s, table := setupTestTable(t)
	eng := openTableEngine(t, s, table)
	db := eng.DB()
	iter, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	engine, err := newRowFilterEngine(&bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_RowKeyRegexFilter{
			RowKeyRegexFilter: []byte("^row1$"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !engine.hasMatch(iter) {
		t.Fatal("expected hasMatch to return true for row1")
	}
}

func TestFilterEngineHasMatchNoMatch(t *testing.T) {
	s, table := setupTestTable(t)
	eng := openTableEngine(t, s, table)
	db := eng.DB()
	iter, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	engine, err := newRowFilterEngine(&bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_RowKeyRegexFilter{
			RowKeyRegexFilter: []byte("^nonexistent$"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if engine.hasMatch(iter) {
		t.Fatal("expected hasMatch to return false for nonexistent row")
	}
}

func TestRowFilterEngineMatchesCell(t *testing.T) {
	engine, err := newRowFilterEngine(&bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_RowKeyRegexFilter{
			RowKeyRegexFilter: []byte("^row1$"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !engine.matchesCell([]byte("row1"), "cf", []byte("q"), 100, []byte("v")) {
		t.Fatal("should match row1")
	}
	if engine.matchesCell([]byte("row2"), "cf", []byte("q"), 100, []byte("v")) {
		t.Fatal("should not match row2")
	}
}

func TestBuildColumnRangeFilterFromProto(t *testing.T) {
	cr := &bigtablepb.ColumnRange{
		FamilyName: "cf",
		StartQualifier: &bigtablepb.ColumnRange_StartQualifierClosed{
			StartQualifierClosed: []byte("a"),
		},
		EndQualifier: &bigtablepb.ColumnRange_EndQualifierClosed{
			EndQualifierClosed: []byte("z"),
		},
	}
	f := buildColumnRangeFilter(cr)
	if f.family != "cf" {
		t.Fatalf("family: got %q", f.family)
	}
	if !f.startInclusive || !f.endInclusive {
		t.Fatal("expected inclusive bounds")
	}
	if !bytes.Equal(f.startQualifier, []byte("a")) {
		t.Fatal("start qualifier mismatch")
	}
	if !bytes.Equal(f.endQualifier, []byte("z")) {
		t.Fatal("end qualifier mismatch")
	}
}

func TestBuildColumnRangeFilterFromProtoOpen(t *testing.T) {
	cr := &bigtablepb.ColumnRange{
		FamilyName: "cf",
		StartQualifier: &bigtablepb.ColumnRange_StartQualifierOpen{
			StartQualifierOpen: []byte("a"),
		},
		EndQualifier: &bigtablepb.ColumnRange_EndQualifierOpen{
			EndQualifierOpen: []byte("z"),
		},
	}
	f := buildColumnRangeFilter(cr)
	if f.startInclusive || f.endInclusive {
		t.Fatal("expected non-inclusive bounds")
	}
}

func TestBuildConditionFilterFromProto(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_Condition_{
			Condition: &bigtablepb.RowFilter_Condition{
				PredicateFilter: &bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
				TrueFilter:      &bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_BlockAllFilter{}},
				FalseFilter:     &bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_PassAllFilter{}},
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.evaluate(cellInfo{}) {
		t.Fatal("predicate matches → true filter (blockAll) should block")
	}
}

func TestBuildTimestampRangeFilter(t *testing.T) {
	tr := &bigtablepb.TimestampRange{
		StartTimestampMicros: 100,
		EndTimestampMicros:   200,
	}
	f := buildTimestampRangeFilter(tr)
	if f.startMicros != 100 || f.endMicros != 200 {
		t.Fatal("timestamp range mismatch")
	}
}

func TestBuildTimestampRangeFilterDefaults(t *testing.T) {
	tr := &bigtablepb.TimestampRange{}
	f := buildTimestampRangeFilter(tr)
	if f.startMicros != 0 || f.endMicros != 0 {
		t.Fatal("expected zero defaults")
	}
}

func TestValueRegexFilter(t *testing.T) {
	f := &valueRegexFilter{re: regexp.MustCompile("^hello")}
	if !f.evaluate(cellInfo{value: []byte("hello world")}) {
		t.Fatal("should match 'hello world'")
	}
	if f.evaluate(cellInfo{value: []byte("world hello")}) {
		t.Fatal("should not match 'world hello'")
	}
}

func TestBuildEvaluatorValueRegex(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_ValueRegexFilter{
			ValueRegexFilter: []byte("^v[12]$"),
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{value: []byte("v1")}) {
		t.Fatal("should match v1")
	}
	if e.evaluate(cellInfo{value: []byte("v3")}) {
		t.Fatal("should not match v3")
	}
}

func TestBuildEvaluatorValueRegexInvalid(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_ValueRegexFilter{
			ValueRegexFilter: []byte("[invalid"),
		},
	}
	_, err := buildEvaluator(f)
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
}

func TestValueRangeFilter(t *testing.T) {
	f := &valueRangeFilter{startValue: []byte("b"), startInclusive: true, endValue: []byte("d"), endInclusive: false}
	if f.evaluate(cellInfo{value: []byte("a")}) {
		t.Fatal("should exclude 'a' (before start)")
	}
	if !f.evaluate(cellInfo{value: []byte("b")}) {
		t.Fatal("should include 'b' (closed start)")
	}
	if !f.evaluate(cellInfo{value: []byte("c")}) {
		t.Fatal("should include 'c' (within range)")
	}
	if f.evaluate(cellInfo{value: []byte("d")}) {
		t.Fatal("should exclude 'd' (open end)")
	}
	if f.evaluate(cellInfo{value: []byte("e")}) {
		t.Fatal("should exclude 'e' (after end)")
	}
}

func TestBuildEvaluatorValueRange(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_ValueRangeFilter{
			ValueRangeFilter: &bigtablepb.ValueRange{
				StartValue: &bigtablepb.ValueRange_StartValueClosed{
					StartValueClosed: []byte("v2"),
				},
				EndValue: &bigtablepb.ValueRange_EndValueOpen{
					EndValueOpen: []byte("v4"),
				},
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !e.evaluate(cellInfo{value: []byte("v2")}) {
		t.Fatal("should include v2 (closed start)")
	}
	if !e.evaluate(cellInfo{value: []byte("v3")}) {
		t.Fatal("should include v3")
	}
	if e.evaluate(cellInfo{value: []byte("v4")}) {
		t.Fatal("should exclude v4 (open end)")
	}
}

func TestValueBitmaskFilter(t *testing.T) {
	f := &valueBitmaskFilter{mask: []byte{0x0F}}
	if !f.evaluate(cellInfo{value: []byte{0x1F}}) {
		t.Fatal("0x1F & 0x0F == 0x0F, should match")
	}
	if f.evaluate(cellInfo{value: []byte{0xF0}}) {
		t.Fatal("0xF0 & 0x0F == 0x00 != 0x0F, should not match")
	}
	if f.evaluate(cellInfo{value: []byte{0x0F, 0x01}}) {
		t.Fatal("remaining bytes must be zero, should not match")
	}
	if f.evaluate(cellInfo{value: []byte{0x01}}) {
		t.Fatal("value shorter than mask should not match")
	}
}

func TestBuildEvaluatorValueBitmask(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_ValueBitmaskFilter{
			ValueBitmaskFilter: &bigtablepb.ValueBitmask{
				Mask: []byte{0xFF, 0x00},
			},
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.evaluate(cellInfo{value: []byte{0x01, 0x00}}) {
		t.Fatal("0x01 & 0xFF != 0xFF, should not match")
	}
	if !e.evaluate(cellInfo{value: []byte{0xFF, 0x00}}) {
		t.Fatal("0xFF & 0xFF == 0xFF and trailing ok, should match")
	}
}

func TestRowSampleFilter(t *testing.T) {
	f := &rowSampleFilter{rate: 1.0}
	if !f.evaluate(cellInfo{rowKey: []byte("row1")}) {
		t.Fatal("rate=1.0 should always sample")
	}
	f2 := &rowSampleFilter{rate: 0.0}
	if f2.evaluate(cellInfo{rowKey: []byte("row1")}) {
		t.Fatal("rate=0.0 should never sample")
	}
}

func TestBuildEvaluatorRowSample(t *testing.T) {
	f := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_RowSampleFilter{
			RowSampleFilter: 0.5,
		},
	}
	e, err := buildEvaluator(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Can't assert exact behavior of random sampling, just check it doesn't panic.
	_ = e.evaluate(cellInfo{rowKey: []byte("row1")})
	e.reset()
}

func TestValueRegexFilterViaReadRows(t *testing.T) {
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/t"
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Filter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_ValueRegexFilter{
				ValueRegexFilter: []byte("^v[12]$"),
			},
		},
	}
	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	// Should find v1 and v2 (match ^v[12]$), no v3/v4/v5.
	valueCount := 0
	for _, c := range allChunks {
		if c.GetCommitRow() {
			continue
		}
		valueCount++
		v := string(c.Value)
		if v != "v1" && v != "v2" {
			t.Fatalf("unexpected value %q", v)
		}
	}
	if valueCount != 2 {
		t.Fatalf("expected 2 matching cells, got %d", valueCount)
	}
}

func TestCellsPerColumnLimitFilterReset(t *testing.T) {
	f := &cellsPerColumnLimitFilter{limit: 1}
	f.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 1})
	f.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 2})
	f.reset()
	if !f.evaluate(cellInfo{family: "cf", qualifier: []byte("q"), ts: 3}) {
		t.Fatal("after reset, cell should pass")
	}
}
