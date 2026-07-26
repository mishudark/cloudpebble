package engine

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
)

func TestApplyDefaultTableFilterPolicy(t *testing.T) {
	opts := &pebble.Options{}
	applyDefaultTableFilterPolicy(opts)
	opts.EnsureDefaults()

	want := bloom.FilterPolicy(10).Name()
	for i := range opts.Levels {
		if got := opts.Levels[i].TableFilterPolicy().Name(); got != want {
			t.Fatalf("level %d: expected filter policy %q, got %q", i, want, got)
		}
	}
}

func TestApplyDefaultTableFilterPolicyHonorsExplicitOptOut(t *testing.T) {
	opts := &pebble.Options{}
	for i := range opts.Levels {
		opts.Levels[i].TableFilterPolicy = func() pebble.TableFilterPolicy {
			return pebble.NoFilterPolicy
		}
	}
	applyDefaultTableFilterPolicy(opts)
	opts.EnsureDefaults()

	for i := range opts.Levels {
		if got := opts.Levels[i].TableFilterPolicy(); got != pebble.NoFilterPolicy {
			t.Fatalf("level %d: explicit NoFilterPolicy overridden with %q", i, got.Name())
		}
	}
}

func TestApplyDefaultTableFilterPolicyRespectsCaller(t *testing.T) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	custom := bloom.FilterPolicy(8)
	for i := range opts.Levels {
		opts.Levels[i].TableFilterPolicy = func() pebble.TableFilterPolicy {
			return custom
		}
	}
	applyDefaultTableFilterPolicy(opts)

	for i := range opts.Levels {
		if got := opts.Levels[i].TableFilterPolicy().Name(); got != custom.Name() {
			t.Fatalf("level %d: caller policy overridden: expected %q, got %q", i, custom.Name(), got)
		}
	}
}
