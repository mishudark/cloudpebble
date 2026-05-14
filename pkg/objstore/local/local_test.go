package local_test

import (
	"testing"

	"github.com/mishudark/cloudpebble/pkg/objstore"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
	"github.com/mishudark/cloudpebble/pkg/objstore/testutil"
)

func TestLocalContract(t *testing.T) {
	testutil.RunContractTests(t, func(tb testing.TB) objstore.Store {
		dir := t.TempDir()
		s, err := local.New(dir)
		if err != nil {
			tb.Fatal(err)
		}
		return s
	})
}
