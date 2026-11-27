package checkpoint

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"bog/internal/store"
)

func TestPackMergeRows(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		sizes            []int
		capacity         int
		minimum, maximum int
		groups           [][]int
	}{
		{
			name: "equal sizes and equal rooms prefer lower indexes", sizes: []int{40, 60, 40, 60},
			capacity: 100, minimum: 2, maximum: 2, groups: [][]int{{1, 0}, {3, 2}},
		},
		{
			name: "tightest fitting output", sizes: []int{25, 65, 110, 140},
			capacity: 200, minimum: 2, maximum: 2, groups: [][]int{{3}, {2, 1, 0}},
		},
		{
			name: "retry within output limit", sizes: []int{60, 60, 60, 60},
			capacity: 100, minimum: 3, maximum: mergeFanIn, groups: [][]int{{0}, {1}, {2}, {3}},
		},
		{
			name: "use smallest viable count", sizes: []int{60, 40, 100},
			capacity: 100, minimum: 2, maximum: mergeFanIn, groups: [][]int{{2}, {0, 1}},
		},
		{
			name: "aggregate capacity cannot fit row shapes", sizes: []int{60, 60, 60},
			capacity: 100, minimum: 2, maximum: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			checkPackingLayout(t, tc.sizes, tc.groups, func(rows []BlockRow) ([][]BlockRow, error) {
				return packMergeRows(t.Context(), rows, tc.capacity, tc.minimum, tc.maximum)
			})
		})
	}
}

func TestPlaceInSlack(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		sizes  []int
		rooms  []int
		groups [][]int
	}{
		{
			name: "reuse a gap before opening an equal one", sizes: []int{40, 60},
			rooms: []int{100, 100}, groups: [][]int{{1, 0}},
		},
		{
			name: "used gaps with equal room prefer earlier gap", sizes: []int{25, 30, 55, 60},
			rooms: []int{100, 95, 80}, groups: [][]int{{3, 1}, {2, 0}},
		},
		{
			name: "prefer widest used gap", sizes: []int{25, 35, 60, 65},
			rooms: []int{100, 100}, groups: [][]int{{3, 0}, {2, 1}},
		},
		{
			name: "exceed original repack fan-in", sizes: []int{40, 40, 40, 40, 40},
			rooms: []int{40, 40, 40, 40, 40}, groups: [][]int{{0}, {1}, {2}, {3}, {4}},
		},
		{
			name: "unopened gap too small despite aggregate room", sizes: []int{50, 60},
			rooms: []int{100, 30},
		},
		{
			name: "all gaps exhausted", sizes: []int{60, 60}, rooms: []int{100},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rooms := slices.Clone(tc.rooms)
			checkPackingLayout(t, tc.sizes, tc.groups, func(rows []BlockRow) ([][]BlockRow, error) {
				return placeInSlack(t.Context(), rows, rooms)
			})
			if !slices.Equal(rooms, tc.rooms) {
				t.Fatalf("packing changed the supplied capacities to %v, want %v", rooms, tc.rooms)
			}
		})
	}
}

func checkPackingLayout(t *testing.T, sizes []int, groups [][]int, pack func([]BlockRow) ([][]BlockRow, error)) {
	t.Helper()
	original := make([]BlockRow, len(sizes))
	for i, size := range sizes {
		original[i] = BlockRow{
			Row: store.Row{Line: strings.Repeat("a", size-blockRowSize)}, Ordinal: uint64(i),
		}
	}
	for _, reverse := range []bool{false, true} {
		rows := slices.Clone(original)
		if reverse {
			slices.Reverse(rows)
		}
		layout, err := pack(rows)
		if groups == nil {
			if layout != nil || !errors.Is(err, errSegmentCapacity) {
				t.Fatalf("layout = %v, error = %v, want no layout and capacity error", layout, err)
			}
			continue
		}
		if err != nil || len(layout) != len(groups) {
			t.Fatalf("got %d groups and error %v, want %d groups", len(layout), err, len(groups))
		}
		offset := 0
		for i, group := range layout {
			if len(group) != len(groups[i]) || cap(group) != len(group) || &group[0] != &rows[offset] {
				t.Fatalf("group %d does not occupy its own contiguous range of %d input rows", i, len(groups[i]))
			}
			for j, rowIndex := range groups[i] {
				if group[j] != original[rowIndex] {
					t.Fatalf("group %d row %d = %+v, want %+v", i, j, group[j], original[rowIndex])
				}
			}
			offset += len(group)
		}
		if offset != len(rows) {
			t.Fatalf("grouped %d of %d rows", offset, len(rows))
		}
	}
}

func TestMergePackingCancellation(t *testing.T) {
	t.Parallel()
	const count = 1025
	const capacity = count * blockRowSize
	for _, tc := range []struct {
		name string
		pack func(context.Context, []BlockRow) ([][]BlockRow, error)
	}{
		{"repack", func(ctx context.Context, rows []BlockRow) ([][]BlockRow, error) {
			return packMergeRows(ctx, rows, capacity, 1, 1)
		}},
		{"slack", func(ctx context.Context, rows []BlockRow) ([][]BlockRow, error) {
			return placeInSlack(ctx, rows, []int{capacity})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, trial := range []struct {
				rows     int
				cancelAt int
				polls    int
				wantErr  error
			}{
				{count, 1, 1, context.Canceled},
				{count - 1, 2, 1, nil},
				{count, 2, 2, context.Canceled},
			} {
				ctx, cancel := context.WithCancel(t.Context())
				polling := &packingPollContext{Context: ctx, cancel: cancel, cancelAt: trial.cancelAt}
				rows := make([]BlockRow, trial.rows)
				for i := range rows {
					rows[i].Ordinal = uint64(i)
				}
				layout, err := tc.pack(polling, rows)
				cancel()
				if !errors.Is(err, trial.wantErr) || polling.polls != trial.polls {
					t.Fatalf("%d rows, cancel at poll %d: got error %v after %d polls, want %v after %d polls",
						trial.rows, trial.cancelAt, err, polling.polls, trial.wantErr, trial.polls)
				}
				if trial.wantErr == nil {
					if len(layout) != 1 || len(layout[0]) != trial.rows {
						t.Fatalf("packing %d rows did not return a complete group", trial.rows)
					}
				} else if layout != nil {
					t.Fatal("canceled packing returned a layout")
				}
			}
		})
	}
}

// packingPollContext cancels on a specific check without depending on timing.
type packingPollContext struct {
	context.Context
	cancel   context.CancelFunc
	cancelAt int
	polls    int
}

func (c *packingPollContext) Err() error {
	c.polls++
	if c.polls == c.cancelAt {
		c.cancel()
	}
	return c.Context.Err()
}
