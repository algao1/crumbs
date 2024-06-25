package lsm

import (
	"unsafe"
)

// // The use of these global values is probably not
// // the best idea. But it was outlined in the paper.
// var nullNode, deleted, last *AANode

type AATree struct {
	root    *AANode
	deleted *AANode
	last    *AANode
	size    int
	nodes   int

	// This effectively acts like a constant. Not sure why the
	// implementation doesn't use a null pointer?
	nullNode *AANode
}

func NewAATree() *AATree {
	nullNode := &AANode{
		Level: 0,
	}
	nullNode.Left = nullNode
	nullNode.Right = nullNode

	return &AATree{
		root:     nullNode,
		nullNode: nullNode,
		deleted:  nullNode,
		last:     nullNode,
	}
}

func (aa *AATree) Size() int {
	return aa.size
}

func (aa *AATree) Nodes() int {
	return aa.nodes
}

func (aa *AATree) Find(key string) ([]byte, bool) {
	return aa.find(key, aa.root)
}

func (aa *AATree) Insert(key string, val []byte) {
	aa.root = aa.insert(aa.root, key, val)
}

func (aa *AATree) Remove(key string) {
	aa.root = aa.remove(aa.root, key)
}

func (aa *AATree) Traverse(f func(k string, v []byte)) {
	aa.traverse(f, aa.root)
}

const AANODE_SIZE = uint32(unsafe.Sizeof(AANode{}))

type AANode struct {
	Key   string
	Val   []byte
	Left  *AANode
	Right *AANode
	Level int
}

func (aa *AATree) traverse(f func(k string, v []byte), an *AANode) {
	if an == aa.nullNode {
		return
	}
	aa.traverse(f, an.Left)
	f(an.Key, an.Val)
	aa.traverse(f, an.Right)
}

func (aa *AATree) find(k string, an *AANode) ([]byte, bool) {
	if an == aa.nullNode {
		return nil, false
	}
	if an.Key == k {
		return an.Val, true
	} else if an.Key > k {
		return aa.find(k, an.Left)
	} else {
		return aa.find(k, an.Right)
	}
}

func (aa *AATree) insert(k *AANode, key string, val []byte) *AANode {
	if k == aa.nullNode {
		aa.size += len(key) + len(val) + int(AANODE_SIZE)
		aa.nodes++
		return &AANode{
			Key:   key,
			Val:   val,
			Left:  aa.nullNode,
			Right: aa.nullNode,
			Level: 1,
		}
	}

	if k.Key == key {
		aa.size += (len(val) - len(k.Val))
		k.Val = val
	} else if k.Key > key {
		k.Left = aa.insert(k.Left, key, val)
	} else {
		k.Right = aa.insert(k.Right, key, val)
	}
	k = skew(k)
	k = split(k)

	return k
}

func (aa *AATree) remove(k *AANode, key string) *AANode {
	if k != aa.nullNode {
		// Step 1:
		// Search down the tree, and set pointers last and delete.
		aa.last = k
		if key < k.Key {
			k.Left = aa.remove(k.Left, key)
		} else {
			aa.deleted = k
			k.Right = aa.remove(k.Right, key)
		}

		if k == aa.last {
			// Step 2:
			// At the bottom of the tree we remove the element
			// if it is present.
			if aa.deleted != aa.nullNode && key == aa.deleted.Key {
				aa.size -= len(k.Key) + len(k.Val) + int(AANODE_SIZE)
				aa.nodes--
				aa.deleted.Key = k.Key
				aa.deleted.Val = k.Val
				aa.deleted = aa.nullNode
				_ = aa.deleted // ignore static check warnings
				k = k.Right
			}
		} else {
			// Step 3:
			// Otherwise, we are not at the bottom, and rebalance.
			if k.Left.Level < k.Level-1 || k.Right.Level < k.Level-1 {
				k.Level--
				if k.Right.Level > k.Level {
					k.Right.Level = k.Level
				}
				k = skew(k)
				k.Right = skew(k.Right)
				k.Right.Right = skew(k.Right.Right)
				k = split(k)
				k.Right = split(k.Right)
			}
		}
	}
	return k
}

func rotateRight(k2 *AANode) *AANode {
	k1 := k2.Left
	k2.Left = k1.Right
	k1.Right = k2
	return k1
}

func rotateLeft(k2 *AANode) *AANode {
	k1 := k2.Right
	k2.Right = k1.Left
	k1.Left = k2
	return k1
}

func skew(k *AANode) *AANode {
	if k.Left.Level == k.Level {
		k = rotateRight(k)
	}
	return k
}

func split(k *AANode) *AANode {
	if k.Right.Right.Level == k.Level {
		k = rotateLeft(k)
		k.Level += 1
	}
	return k
}
