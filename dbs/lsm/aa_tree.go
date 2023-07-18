package lsm

import (
	"unsafe"
)

var nullNode, deleted, last *AANode

func init() {
	nullNode = &AANode{
		Level: 0,
	}
	nullNode.Left = nullNode
	nullNode.Right = nullNode
}

type AATree struct {
	Root  *AANode
	size  int
	nodes int
}

func NewAATree() *AATree {
	return &AATree{
		Root: nullNode,
	}
}

func (aa *AATree) Size() int {
	return aa.size
}

func (aa *AATree) Nodes() int {
	return aa.nodes
}

func (aa *AATree) Find(key string) ([]byte, bool) {
	return aa.find(key, aa.Root)
}

func (aa *AATree) Insert(key string, val []byte) {
	aa.Root = aa.insert(aa.Root, key, val)
}

func (aa *AATree) Remove(key string) {
	aa.Root = aa.remove(aa.Root, key)
}

func (aa *AATree) Traverse(f func(k string, v []byte)) {
	aa.traverse(f, aa.Root)
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
	if an == nullNode {
		return
	}
	aa.traverse(f, an.Left)
	f(an.Key, an.Val)
	aa.traverse(f, an.Right)
}

func (aa *AATree) find(k string, an *AANode) ([]byte, bool) {
	if an == nullNode {
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
	if k == nullNode {
		aa.size += len(key) + len(val) + int(AANODE_SIZE)
		aa.nodes++
		return &AANode{
			Key:   key,
			Val:   val,
			Left:  nullNode,
			Right: nullNode,
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
	if k != nullNode {
		// Step 1:
		// Search down the tree, and set pointers last and delete.
		last = k
		if key < k.Key {
			k.Left = aa.remove(k.Left, key)
		} else {
			deleted = k
			k.Right = aa.remove(k.Right, key)
		}

		if k == last {
			// Step 2:
			// At the bottom of the tree we remove the element
			// if it is present.
			if deleted != nullNode && key == deleted.Key {
				aa.size -= len(k.Key) + len(k.Val) + int(AANODE_SIZE)
				aa.nodes--
				deleted.Key = k.Key
				deleted.Val = k.Val
				deleted = nullNode
				_ = deleted // ignore static check warnings
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
