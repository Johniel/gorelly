package bsearch

import (
	"testing"
)

func TestBinarySearchBy(t *testing.T) {
	a := []int{1, 2, 3, 5, 8, 13, 21}

	tests := []struct {
		name     string
		target   int
		expected int
		found    bool
	}{
		{"find 1", 1, 0, true},
		{"not find 0", 0, 0, false},
		{"find 2", 2, 1, true},
		{"find 8", 8, 4, true},
		{"not find 6", 6, 4, false},
		{"find 21", 21, 6, true},
		{"not find 22", 22, 7, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := BinarySearchBy(len(a), func(i int) int {
				if a[i] < tt.target {
					return -1
				} else if a[i] > tt.target {
					return 1
				}
				return 0
			})

			if tt.found {
				if err != nil {
					t.Errorf("expected to find %d, got error: %v", tt.target, err)
				}
				if idx != tt.expected {
					t.Errorf("expected index %d, got %d", tt.expected, idx)
				}
			} else {
				if err == nil {
					t.Errorf("expected not to find %d, but got index %d", tt.target, idx)
				}
				if idx != tt.expected {
					t.Errorf("expected insertion point %d, got %d", tt.expected, idx)
				}
			}
		})
	}
}
