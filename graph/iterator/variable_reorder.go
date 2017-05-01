package iterator

import (
	"errors"

	"github.com/codelingo/cayley/graph"
)

// ReorderIteratorTree ensures that each variable is only ever 'bound' (defined and updated)
// in one place. It also ensures that the 'bind' variable iterator only has Next() called
// on it, whereas the 'use' variable iterators only have Contains() called on them.
func ReorderIteratorTree(it graph.Iterator, names []string) error {
	nameMap := map[string]bool{}
	for _, name := range names {
		nameMap[name] = false
	}

	_, _, err := reorderIteratorTree(it, nameMap)
	return err
}

// Makes the first iterator for a given variable found with a depth first search on the
// iterator tree the 'bind' iterator, and all subsequent iterators 'use' iterators.
// Bind iterators must never be internal subiterators of 'And' iterators (nor children of them),
// because internal subiterators call Contains() and contains is undefined for binders.
// Similarly, use iterators must be the child of internal iterators, because Next() is undefined
// for users.
// Users will never be descendants of primary iterators, but that doesn't mean that they will
// be descendants of internal iterators in the general case. However, CodeLingo queries are so
// full of And iterators that they almost certainly will be.
// TODO(BlakeMScurr) make this return new iterators rather than work in place
func reorderIteratorTree(it graph.Iterator, seen map[string]bool) (bool, bool, error) {
	// Binders are given high priority, and users are given low priority.
	highPriority := false
	lowPriority := false
	if it.Type().String() == "variable" {
		if !seen[it.Describe().Name] {
			seen[it.Describe().Name] = true
			highPriority = true
		} else {
			lowPriority = true
		}
	}

	// Subiterators are collected along with their priorities.
	// The subiterators' priorities are propogated back up the tree.
	subIts := []graph.Iterator{}
	highPriorities := []bool{}
	lowPriorities := []bool{}
	for _, subIt := range it.SubIterators() {
		if subIt != nil {
			p, q, err := reorderIteratorTree(subIt, seen)
			if err != nil {
				return false, false, err
			}
			highPriorities = append(highPriorities, p)
			lowPriorities = append(lowPriorities, q)
			subIts = append(subIts, subIt)
			highPriority = p || highPriority
			lowPriority = q || lowPriority
		}
	}

	if it.Type().String() == "and" {
		// Any high priority subiterators are made the primary iterator.
		swapped := false
		lowSwap := false
		for i, p := range highPriorities {
			if p {
				if swapped {
					// We cannot queries where the bind iterators of two (or more) variables are
					// descendants of different subits of the same And iterator.
					// TODO(BlakeMScurr) Find work around to create allow multiple binders under a single
					// primary iterator.
					return false, false, errors.New("Multiple variables not supported in this arrangement.")
				}

				and := it.(*And)
				and.MakePrimary(i)
				swapped = true
			}
		}

		// Low priority primary iterators are swapped out.
		// If no high priority iterator was made the primary iterator, then a low priority
		// iterator may still be the primary iterator.
		if !swapped {
			if lowPriorities[0] {
				for i, p := range lowPriorities {
					if !p {
						and := it.(*And)
						and.MakePrimary(i)
						lowSwap = true
					}
				}
			}
		}
		return swapped, lowSwap, nil
	}
	return highPriority, lowPriority, nil
}
