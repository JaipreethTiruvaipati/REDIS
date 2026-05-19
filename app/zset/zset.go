package zset

import "sort"

// Node represents a single element in the sorted set.
type Node struct {
	Score  float64
	Member string
}

// ZSet represents a Redis Sorted Set.
// It uses a map for O(1) lookups, and a slice to maintain score ordering.
type ZSet struct {
	nodes []Node
	dict  map[string]float64
}

// New creates a new, empty ZSet.
func New() *ZSet {
	return &ZSet{
		nodes: make([]Node, 0),
		dict:  make(map[string]float64),
	}
}

// Add adds a new member to the sorted set, or updates the score of an existing one.
// It returns 1 if a new member was added, and 0 if an existing member was updated.
func (z *ZSet) Add(score float64, member string) int {
	oldScore, exists := z.dict[member]
	if exists {
		if oldScore == score {
			return 0 // Score is the same, no operation needed
		}

		// Find and remove the old node using binary search
		oldIdx := sort.Search(len(z.nodes), func(i int) bool {
			if z.nodes[i].Score == oldScore {
				return z.nodes[i].Member >= member
			}
			return z.nodes[i].Score > oldScore
		})

		// Remove the element from the slice
		if oldIdx < len(z.nodes) && z.nodes[oldIdx].Member == member {
			z.nodes = append(z.nodes[:oldIdx], z.nodes[oldIdx+1:]...)
		}
	}

	// 1. Add/Update the fast-lookup map
	z.dict[member] = score

	// 2. Add to the ordered slice and keep it sorted
	newNode := Node{Score: score, Member: member}

	// Find the correct insertion index using binary search (O(log N))
	idx := sort.Search(len(z.nodes), func(i int) bool {
		if z.nodes[i].Score == score {
			return z.nodes[i].Member >= member
		}
		return z.nodes[i].Score > score
	})

	// Insert the new node at the calculated index
	z.nodes = append(z.nodes, Node{})    // Expand slice by 1
	copy(z.nodes[idx+1:], z.nodes[idx:]) // Shift elements to the right
	z.nodes[idx] = newNode

	if exists {
		return 0 // 0 members added (1 updated)
	}
	return 1 // 1 member was added
}

// Rank returns the 0-based index of the member in the sorted set.
// It returns -1 if the member does not exist.
func (z *ZSet) Rank(member string) int {
	score, exists := z.dict[member]
	if !exists {
		return -1
	}

	// Use binary search (O(log N)) to find the exact position
	idx := sort.Search(len(z.nodes), func(i int) bool {
		if z.nodes[i].Score == score {
			return z.nodes[i].Member >= member
		}
		return z.nodes[i].Score > score
	})

	if idx < len(z.nodes) && z.nodes[idx].Member == member {
		return idx
	}
	return -1
}

// Range returns a slice of members within the specified rank range (inclusive).
// If start is out of bounds or start > stop, it returns an empty slice.
func (z *ZSet) Range(start, stop int) []string {
	if start < 0 {
		start = 0
	}
	if start >= len(z.nodes) {
		return []string{}
	}
	if stop >= len(z.nodes) {
		stop = len(z.nodes) - 1
	}
	if start > stop {
		return []string{}
	}

	var members []string
	for i := start; i <= stop; i++ {
		members = append(members, z.nodes[i].Member)
	}
	return members
}
