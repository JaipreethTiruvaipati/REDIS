package zset

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

// Add adds a new member to the sorted set.
// It returns 1 if a new member was added, and 0 if an existing member was updated.
func (z *ZSet) Add(score float64, member string) int {
	_, exists := z.dict[member]
	if exists {
		// We'll handle updating existing members in future stages.
		return 0
	}

	// 1. Add to the fast-lookup map
	z.dict[member] = score

	// 2. Add to the ordered slice and keep it sorted
	newNode := Node{Score: score, Member: member}

	// Find the correct insertion index to maintain ascending order
	idx := 0
	for i, node := range z.nodes {
		// Sort by Score first. If scores are equal, sort lexicographically by Member.
		if score < node.Score || (score == node.Score && member < node.Member) {
			break
		}
		idx = i + 1
	}

	// Insert the new node at the calculated index
	z.nodes = append(z.nodes, Node{})    // Expand slice by 1
	copy(z.nodes[idx+1:], z.nodes[idx:]) // Shift elements to the right
	z.nodes[idx] = newNode

	return 1 // 1 member was added
}
