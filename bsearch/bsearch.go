// Package bsearch provides binary search functionality for sorted collections.
package bsearch

// BinarySearchBy performs a binary search on a sorted collection of size elements.
// The comparison function f should return:
//   - negative value if the element at index is less than the target
//   - zero if the element at index equals the target
//   - positive value if the element at index is greater than the target
//
// Returns the index if found, or ErrNotFound with the insertion point if not found.
func BinarySearchBy(size int, f func(int) int) (int, error) {
	left := 0
	right := size
	currentSize := size

	for left < right {
		mid := left + currentSize/2
		cmp := f(mid)
		if cmp < 0 {
			left = mid + 1
		} else if cmp > 0 {
			right = mid
		} else {
			return mid, nil
		}
		currentSize = right - left
	}
	return left, ErrNotFound
}

// ErrNotFound is returned when BinarySearchBy does not find a matching element.
var ErrNotFound = &NotFoundError{}

// NotFoundError represents an error when an element is not found during binary search.
type NotFoundError struct{}

func (e *NotFoundError) Error() string {
	return "not found"
}
