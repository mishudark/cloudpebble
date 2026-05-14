package bigtable

// RowProcessor receives decoded cell data from a row scan.
// OnCell returns false to stop processing early.
type RowProcessor struct {
	// OnCell is called for each cell in the row. The labels slice may be nil.
	// Return false to stop iteration.
	OnCell func(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) bool

	// Matched is set to true when at least one cell is processed.
	Matched bool
}
