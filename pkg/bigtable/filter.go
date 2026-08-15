package bigtable

import (
	"bytes"
	"math/rand/v2"
	"regexp"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
)

// rowFilterEngine evaluates a RowFilter against a Pebble iterator.
type rowFilterEngine struct {
	eval       filterEvaluator
	stripValue bool
	labels     []string
	// sunk is set by sinkFilter evaluators when a cell passes a sink. Such
	// cells are emitted to the output even if the overall filter rejects
	// them. Reset before each cell evaluation.
	sunk bool
}

// newRowFilterEngine creates the evaluation tree for a RowFilter.
func newRowFilterEngine(filter *bigtablepb.RowFilter) (*rowFilterEngine, error) {
	fe := &rowFilterEngine{}
	if filter == nil {
		fe.eval = &passAllFilter{}
		return fe, nil
	}
	eval, err := buildEvaluatorWithSink(filter, &fe.sunk)
	if err != nil {
		return nil, err
	}
	fe.eval = eval
	fe.stripValue = evaluatorHasStripValue(eval)
	fe.labels = evaluatorCollectLabels(eval)
	return fe, nil
}

// cellInfo holds the decoded cell data passed through filter stages.
type cellInfo struct {
	rowKey    []byte
	family    string
	qualifier []byte
	ts        int64
	value     []byte
}

// filterEvaluator decides whether a cell passes the filter.
type filterEvaluator interface {
	evaluate(cell cellInfo) bool
	reset()
}

// buildEvaluator recursively constructs the filter evaluation tree.
func buildEvaluator(filter *bigtablepb.RowFilter) (filterEvaluator, error) {
	var sunk bool
	return buildEvaluatorWithSink(filter, &sunk)
}

// buildEvaluatorWithSink recursively constructs the filter evaluation tree.
// sunk is the shared flag sink filters set on every cell that passes them.
func buildEvaluatorWithSink(filter *bigtablepb.RowFilter, sunk *bool) (filterEvaluator, error) {
	if filter == nil {
		return &passAllFilter{}, nil
	}

	switch f := filter.Filter.(type) {
	case *bigtablepb.RowFilter_Chain_:
		return buildChain(f.Chain, sunk)

	case *bigtablepb.RowFilter_Interleave_:
		return buildInterleave(f.Interleave, sunk)

	case *bigtablepb.RowFilter_Condition_:
		return buildCondition(f.Condition, sunk)

	case *bigtablepb.RowFilter_Sink:
		return &sinkFilter{sunk: sunk}, nil

	case *bigtablepb.RowFilter_PassAllFilter:
		return &passAllFilter{}, nil

	case *bigtablepb.RowFilter_BlockAllFilter:
		return &blockAllFilter{}, nil

	case *bigtablepb.RowFilter_RowKeyRegexFilter:
		re, err := regexp.Compile(string(f.RowKeyRegexFilter))
		if err != nil {
			return nil, err
		}
		return &rowKeyRegexFilter{re: re}, nil

	case *bigtablepb.RowFilter_FamilyNameRegexFilter:
		re, err := regexp.Compile(f.FamilyNameRegexFilter)
		if err != nil {
			return nil, err
		}
		return &familyRegexFilter{re: re}, nil

	case *bigtablepb.RowFilter_ColumnQualifierRegexFilter:
		re, err := regexp.Compile(string(f.ColumnQualifierRegexFilter))
		if err != nil {
			return nil, err
		}
		return &qualifierRegexFilter{re: re}, nil

	case *bigtablepb.RowFilter_ColumnRangeFilter:
		return buildColumnRangeFilter(f.ColumnRangeFilter), nil

	case *bigtablepb.RowFilter_TimestampRangeFilter:
		return buildTimestampRangeFilter(f.TimestampRangeFilter), nil

	case *bigtablepb.RowFilter_CellsPerRowOffsetFilter:
		return &cellsPerRowOffsetFilter{offset: int(f.CellsPerRowOffsetFilter)}, nil

	case *bigtablepb.RowFilter_CellsPerRowLimitFilter:
		return &cellsPerRowLimitFilter{limit: int(f.CellsPerRowLimitFilter)}, nil

	case *bigtablepb.RowFilter_CellsPerColumnLimitFilter:
		return &cellsPerColumnLimitFilter{limit: int(f.CellsPerColumnLimitFilter)}, nil

	case *bigtablepb.RowFilter_StripValueTransformer:
		return &stripValueFilter{}, nil

	case *bigtablepb.RowFilter_ApplyLabelTransformer:
		return &applyLabelFilter{label: f.ApplyLabelTransformer}, nil

	case *bigtablepb.RowFilter_ValueRegexFilter:
		re, err := regexp.Compile(string(f.ValueRegexFilter))
		if err != nil {
			return nil, err
		}
		return &valueRegexFilter{re: re}, nil

	case *bigtablepb.RowFilter_ValueRangeFilter:
		return buildValueRangeFilter(f.ValueRangeFilter), nil

	case *bigtablepb.RowFilter_ValueBitmaskFilter:
		return &valueBitmaskFilter{mask: f.ValueBitmaskFilter.GetMask()}, nil

	case *bigtablepb.RowFilter_RowSampleFilter:
		return &rowSampleFilter{rate: f.RowSampleFilter}, nil

	default:
		// Unsupported filters pass everything.
		return &passAllFilter{}, nil
	}
}

// --- Chain ---

type chainFilter struct {
	filters []filterEvaluator
}

func buildChain(chain *bigtablepb.RowFilter_Chain, sunk *bool) (*chainFilter, error) {
	filters := make([]filterEvaluator, 0, len(chain.GetFilters()))
	for _, f := range chain.GetFilters() {
		eval, err := buildEvaluatorWithSink(f, sunk)
		if err != nil {
			return nil, err
		}
		filters = append(filters, eval)
	}
	return &chainFilter{filters: filters}, nil
}

func (c *chainFilter) evaluate(cell cellInfo) bool {
	for _, f := range c.filters {
		if !f.evaluate(cell) {
			return false
		}
	}
	return true
}

func (c *chainFilter) reset() {
	for _, f := range c.filters {
		f.reset()
	}
}

// --- Interleave ---

type interleaveFilter struct {
	filters []filterEvaluator
	seen    map[cellIdentity]bool
}

type cellIdentity struct {
	family    string
	qualifier string
	ts        int64
}

func buildInterleave(il *bigtablepb.RowFilter_Interleave, sunk *bool) (*interleaveFilter, error) {
	filters := make([]filterEvaluator, 0, len(il.GetFilters()))
	for _, f := range il.GetFilters() {
		eval, err := buildEvaluatorWithSink(f, sunk)
		if err != nil {
			return nil, err
		}
		filters = append(filters, eval)
	}
	return &interleaveFilter{filters: filters, seen: make(map[cellIdentity]bool)}, nil
}

func (il *interleaveFilter) evaluate(cell cellInfo) bool {
	for _, f := range il.filters {
		if f.evaluate(cell) {
			id := cellIdentity{family: cell.family, qualifier: string(cell.qualifier), ts: cell.ts}
			if il.seen == nil {
				il.seen = make(map[cellIdentity]bool)
			}
			if il.seen[id] {
				return false
			}
			il.seen[id] = true
			return true
		}
	}
	return false
}

func (il *interleaveFilter) reset() {
	il.seen = make(map[cellIdentity]bool)
	for _, f := range il.filters {
		f.reset()
	}
}

// --- Condition ---

type conditionFilter struct {
	predicate   filterEvaluator
	trueFilter  filterEvaluator
	falseFilter filterEvaluator
	matched     bool
	evaluated   bool
}

func buildCondition(cond *bigtablepb.RowFilter_Condition, sunk *bool) (*conditionFilter, error) {
	pred, err := buildEvaluatorWithSink(cond.GetPredicateFilter(), sunk)
	if err != nil {
		return nil, err
	}
	cf := &conditionFilter{predicate: pred}
	if cond.GetTrueFilter() != nil {
		tf, err := buildEvaluatorWithSink(cond.GetTrueFilter(), sunk)
		if err != nil {
			return nil, err
		}
		cf.trueFilter = tf
	}
	if cond.GetFalseFilter() != nil {
		ff, err := buildEvaluatorWithSink(cond.GetFalseFilter(), sunk)
		if err != nil {
			return nil, err
		}
		cf.falseFilter = ff
	}
	return cf, nil
}

func (c *conditionFilter) evaluate(cell cellInfo) bool {
	if c.predicate.evaluate(cell) {
		if c.trueFilter != nil {
			return c.trueFilter.evaluate(cell)
		}
		return true
	}
	if c.falseFilter != nil {
		return c.falseFilter.evaluate(cell)
	}
	return false
}

func (c *conditionFilter) reset() {
	c.matched = false
	c.evaluated = false
	c.predicate.reset()
	if c.trueFilter != nil {
		c.trueFilter.reset()
	}
	if c.falseFilter != nil {
		c.falseFilter.reset()
	}
}

// --- Sink ---

// sinkFilter emits every cell that reaches it to the output (by setting the
// shared sunk flag) while always passing the cell through to the rest of
// the chain. This lets clients tee off intermediate results of a filter
// chain, e.g. chain{sink, blockAll} streams all scanned cells.
type sinkFilter struct {
	sunk *bool
}

func (s *sinkFilter) evaluate(cell cellInfo) bool {
	*s.sunk = true
	return true
}
func (s *sinkFilter) reset() {}

// --- Pass / Block ---

type passAllFilter struct{}

func (p *passAllFilter) evaluate(cell cellInfo) bool { return true }
func (p *passAllFilter) reset()                      {}

type blockAllFilter struct{}

func (b *blockAllFilter) evaluate(cell cellInfo) bool { return false }
func (b *blockAllFilter) reset()                      {}

// --- Regex Filters ---

type rowKeyRegexFilter struct {
	re *regexp.Regexp
}

func (r *rowKeyRegexFilter) evaluate(cell cellInfo) bool { return r.re.Match(cell.rowKey) }
func (r *rowKeyRegexFilter) reset()                      {}

type familyRegexFilter struct {
	re *regexp.Regexp
}

func (r *familyRegexFilter) evaluate(cell cellInfo) bool { return r.re.MatchString(cell.family) }
func (r *familyRegexFilter) reset()                      {}

type qualifierRegexFilter struct {
	re *regexp.Regexp
}

func (r *qualifierRegexFilter) evaluate(cell cellInfo) bool { return r.re.Match(cell.qualifier) }
func (r *qualifierRegexFilter) reset()                      {}

// --- Column Range Filter ---

type columnRangeFilter struct {
	family         string
	startQualifier []byte
	endQualifier   []byte
	startInclusive bool
	endInclusive   bool
	rangeSet       bool
}

func buildColumnRangeFilter(cr *bigtablepb.ColumnRange) *columnRangeFilter {
	f := &columnRangeFilter{family: cr.GetFamilyName()}
	switch s := cr.StartQualifier.(type) {
	case *bigtablepb.ColumnRange_StartQualifierClosed:
		f.startQualifier = s.StartQualifierClosed
		f.startInclusive = true
		f.rangeSet = true
	case *bigtablepb.ColumnRange_StartQualifierOpen:
		f.startQualifier = s.StartQualifierOpen
		f.rangeSet = true
	}
	switch e := cr.EndQualifier.(type) {
	case *bigtablepb.ColumnRange_EndQualifierClosed:
		f.endQualifier = e.EndQualifierClosed
		f.endInclusive = true
		f.rangeSet = true
	case *bigtablepb.ColumnRange_EndQualifierOpen:
		f.endQualifier = e.EndQualifierOpen
		f.rangeSet = true
	}
	return f
}

func (c *columnRangeFilter) evaluate(cell cellInfo) bool {
	if c.family != "" && cell.family != c.family {
		return false
	}
	if !c.rangeSet {
		return true
	}
	if len(c.startQualifier) > 0 {
		cmp := bytes.Compare(cell.qualifier, c.startQualifier)
		if c.startInclusive {
			if cmp < 0 {
				return false
			}
		} else {
			if cmp <= 0 {
				return false
			}
		}
	}
	if len(c.endQualifier) > 0 {
		cmp := bytes.Compare(cell.qualifier, c.endQualifier)
		if c.endInclusive {
			if cmp > 0 {
				return false
			}
		} else {
			if cmp >= 0 {
				return false
			}
		}
	}
	return true
}

func (c *columnRangeFilter) reset() {}

// --- Timestamp Range Filter ---

type timestampRangeFilter struct {
	startMicros int64
	endMicros   int64
}

func buildTimestampRangeFilter(tr *bigtablepb.TimestampRange) *timestampRangeFilter {
	return &timestampRangeFilter{
		startMicros: tr.GetStartTimestampMicros(),
		endMicros:   tr.GetEndTimestampMicros(),
	}
}

func (t *timestampRangeFilter) evaluate(cell cellInfo) bool {
	if t.startMicros > 0 && cell.ts < t.startMicros {
		return false
	}
	if t.endMicros > 0 && cell.ts >= t.endMicros {
		return false
	}
	return true
}

func (t *timestampRangeFilter) reset() {}

// --- Cell Count Filters ---

type cellsPerRowOffsetFilter struct {
	offset   int
	rowCount int
}

func (c *cellsPerRowOffsetFilter) evaluate(cell cellInfo) bool {
	c.rowCount++
	return c.rowCount > c.offset
}

func (c *cellsPerRowOffsetFilter) reset() { c.rowCount = 0 }

type cellsPerRowLimitFilter struct {
	limit    int
	rowCount int
}

func (c *cellsPerRowLimitFilter) evaluate(cell cellInfo) bool {
	if c.rowCount >= c.limit {
		return false
	}
	c.rowCount++
	return true
}

func (c *cellsPerRowLimitFilter) reset() { c.rowCount = 0 }

type cellsPerColumnLimitFilter struct {
	limit     int
	colCounts map[string]map[string]int
}

func (c *cellsPerColumnLimitFilter) evaluate(cell cellInfo) bool {
	if c.colCounts == nil {
		c.colCounts = make(map[string]map[string]int)
	}
	quals := c.colCounts[cell.family]
	if quals == nil {
		quals = make(map[string]int)
		c.colCounts[cell.family] = quals
	}
	col := string(cell.qualifier)
	n := quals[col]
	if n >= c.limit {
		return false
	}
	quals[col] = n + 1
	return true
}

func (c *cellsPerColumnLimitFilter) reset() { c.colCounts = nil }

// --- Transformers ---

type stripValueFilter struct{}

func (s *stripValueFilter) evaluate(cell cellInfo) bool {
	// Value is stripped; this is handled by the caller.
	return true
}
func (s *stripValueFilter) reset() {}

type applyLabelFilter struct {
	label string
}

func (a *applyLabelFilter) evaluate(cell cellInfo) bool {
	return true
}
func (a *applyLabelFilter) reset() {}

// --- Value Regex Filter ---

type valueRegexFilter struct {
	re *regexp.Regexp
}

func (v *valueRegexFilter) evaluate(cell cellInfo) bool {
	return v.re.Match(cell.value)
}
func (v *valueRegexFilter) reset() {}

// --- Value Range Filter ---

type valueRangeFilter struct {
	startValue     []byte
	endValue       []byte
	startInclusive bool
	endInclusive   bool
}

func buildValueRangeFilter(vr *bigtablepb.ValueRange) *valueRangeFilter {
	f := &valueRangeFilter{}
	switch s := vr.StartValue.(type) {
	case *bigtablepb.ValueRange_StartValueClosed:
		f.startValue = s.StartValueClosed
		f.startInclusive = true
	case *bigtablepb.ValueRange_StartValueOpen:
		f.startValue = s.StartValueOpen
	}
	switch e := vr.EndValue.(type) {
	case *bigtablepb.ValueRange_EndValueClosed:
		f.endValue = e.EndValueClosed
		f.endInclusive = true
	case *bigtablepb.ValueRange_EndValueOpen:
		f.endValue = e.EndValueOpen
	}
	return f
}

func (v *valueRangeFilter) evaluate(cell cellInfo) bool {
	if len(v.startValue) > 0 {
		cmp := bytes.Compare(cell.value, v.startValue)
		if v.startInclusive {
			if cmp < 0 {
				return false
			}
		} else {
			if cmp <= 0 {
				return false
			}
		}
	}
	if len(v.endValue) > 0 {
		cmp := bytes.Compare(cell.value, v.endValue)
		if v.endInclusive {
			if cmp > 0 {
				return false
			}
		} else {
			if cmp >= 0 {
				return false
			}
		}
	}
	return true
}
func (v *valueRangeFilter) reset() {}

// --- Value Bitmask Filter ---

type valueBitmaskFilter struct {
	mask []byte
}

func (v *valueBitmaskFilter) evaluate(cell cellInfo) bool {
	if len(cell.value) < len(v.mask) {
		return false
	}
	for i := 0; i < len(v.mask); i++ {
		if (cell.value[i] & v.mask[i]) != v.mask[i] {
			return false
		}
	}
	// Remaining bytes (beyond mask length) must be zero.
	for i := len(v.mask); i < len(cell.value); i++ {
		if cell.value[i] != 0 {
			return false
		}
	}
	return true
}
func (v *valueBitmaskFilter) reset() {}

// --- Row Sample Filter ---

type rowSampleFilter struct {
	rate      float64
	seenRow   []byte
	sampleRow bool
}

func (r *rowSampleFilter) evaluate(cell cellInfo) bool {
	if !bytes.Equal(cell.rowKey, r.seenRow) {
		r.seenRow = append(r.seenRow[:0], cell.rowKey...)
		r.sampleRow = randFloat64() < r.rate
	}
	return r.sampleRow
}
func (r *rowSampleFilter) reset() {
	r.seenRow = r.seenRow[:0]
}

func randFloat64() float64 {
	return rand.Float64() //nolint:gosec // statistical sampling, not cryptographic
}

// --- Filter engine methods ---

// evaluateCell evaluates the filter for a single cell, honoring sink
// side effects: a cell that passed a sink counts as emitted even when the
// filter's final verdict rejects it.
func (e *rowFilterEngine) evaluateCell(cell cellInfo) bool {
	e.sunk = false
	matched := e.eval.evaluate(cell)
	return matched || e.sunk
}

// hasMatch returns true if any cell in the iterator matches the filter.
func (e *rowFilterEngine) hasMatch(iter *pebble.Iterator) bool {
	e.eval.reset()
	hasAny := false
	var dec CellDecoder
	iter.First()
	for ; iter.Valid(); iter.Next() {
		rowKey, family, qualifier, ts, ok := dec.Decode(iter.Key())
		if !ok {
			continue
		}
		val := iter.Value()
		if e.evaluateCell(cellInfo{rowKey: rowKey, family: family, qualifier: qualifier, ts: ts, value: val}) {
			hasAny = true
			break
		}
	}
	return hasAny
}

// matchesCell evaluates the filter for a single cell.
func (e *rowFilterEngine) matchesCell(rowKey []byte, family string, qualifier []byte, ts int64, value []byte) bool {
	return e.evaluateCell(cellInfo{
		rowKey:    rowKey,
		family:    family,
		qualifier: qualifier,
		ts:        ts,
		value:     value,
	})
}

// hasStripValue reports whether the filter engine contains a strip-value
// transformer. Uses the precomputed cached field.
func (e *rowFilterEngine) hasStripValue() bool {
	return e.stripValue
}

func evaluatorHasStripValue(e filterEvaluator) bool {
	switch f := e.(type) {
	case *stripValueFilter:
		return true
	case *chainFilter:
		if slices.ContainsFunc(f.filters, evaluatorHasStripValue) {
			return true
		}
	case *interleaveFilter:
		if slices.ContainsFunc(f.filters, evaluatorHasStripValue) {
			return true
		}
	case *conditionFilter:
		if f.predicate != nil && evaluatorHasStripValue(f.predicate) {
			return true
		}
		if f.trueFilter != nil && evaluatorHasStripValue(f.trueFilter) {
			return true
		}
		if f.falseFilter != nil && evaluatorHasStripValue(f.falseFilter) {
			return true
		}
	}
	return false
}

// process iterates over the iterator, calling rp.OnCell for each cell that passes
// the filter. Returns false if processing was stopped early.
func (e *rowFilterEngine) process(iter *pebble.Iterator, rp *RowProcessor) {
	e.eval.reset()
	stripValue := e.stripValue
	labels := e.labels

	var dec CellDecoder
	var cell processCell

	for iter.First(); iter.Valid(); iter.Next() {
		rowKey, family, qualifier, ts, ok := dec.Decode(iter.Key())
		if !ok {
			continue
		}

		val := iter.Value()
		if len(val) > 0 && !stripValue {
			cell.valueBuf = append(cell.valueBuf[:0], val...)
			val = cell.valueBuf
		}

		if !e.evaluateCell(cellInfo{rowKey: rowKey, family: family, qualifier: qualifier, ts: ts, value: val}) {
			continue
		}

		if stripValue {
			val = nil
		}

		// Copy rowKey and qualifier so the callback can retain them.
		cell.rowKeyBuf = append(cell.rowKeyBuf[:0], rowKey...)
		cell.qualBuf = append(cell.qualBuf[:0], qualifier...)

		rp.Matched = true
		if !rp.OnCell(cell.rowKeyBuf, family, cell.qualBuf, ts, val, labels) {
			return
		}
	}
}

// processCell reuses buffers across process iterations.
type processCell struct {
	rowKeyBuf  []byte
	qualBuf    []byte
	valueBuf   []byte
}

// evaluatorCollectLabels returns the labels from all applyLabelFilter instances
// in the evaluation tree.
func evaluatorCollectLabels(e filterEvaluator) []string {
	switch f := e.(type) {
	case *applyLabelFilter:
		return []string{f.label}
	case *chainFilter:
		var labels []string
		for _, sub := range f.filters {
			labels = append(labels, evaluatorCollectLabels(sub)...)
		}
		return labels
	case *interleaveFilter:
		var labels []string
		for _, sub := range f.filters {
			labels = append(labels, evaluatorCollectLabels(sub)...)
		}
		return labels
	case *conditionFilter:
		var labels []string
		if f.predicate != nil {
			labels = append(labels, evaluatorCollectLabels(f.predicate)...)
		}
		if f.trueFilter != nil {
			labels = append(labels, evaluatorCollectLabels(f.trueFilter)...)
		}
		if f.falseFilter != nil {
			labels = append(labels, evaluatorCollectLabels(f.falseFilter)...)
		}
		return labels
	}
	return nil
}
