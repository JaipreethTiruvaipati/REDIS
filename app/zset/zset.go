package zset

import "sort"

type ZSetNode struct {
	Member string
	Score  float64
}

type ZSet struct {
	Dict map[string]float64
	List []ZSetNode
}

func NewZSet() *ZSet {
	return &ZSet{
		Dict: make(map[string]float64),
		List: make([]ZSetNode, 0),
	}
}

// Add adds or updates a member. Returns true if it was a new member.
func (zs *ZSet) Add(score float64, member string) bool {
	oldScore, exists := zs.Dict[member]
	if exists {
		if oldScore == score {
			return false // No change
		}
		// Remove old node from List
		for i, node := range zs.List {
			if node.Member == member {
				zs.List = append(zs.List[:i], zs.List[i+1:]...)
				break
			}
		}
	}
	
	zs.Dict[member] = score

	// Insert into List in sorted order
	// Primary sort by score, secondary sort by lex order of member
	newNode := ZSetNode{Member: member, Score: score}
	zs.List = append(zs.List, newNode)
	sort.Slice(zs.List, func(i, j int) bool {
		if zs.List[i].Score == zs.List[j].Score {
			return zs.List[i].Member < zs.List[j].Member
		}
		return zs.List[i].Score < zs.List[j].Score
	})

	return !exists
}
