package structures

import (
	"hash/fnv"
	"math/rand"
	"time"
)

const (
	maxl = 32
	p    = 0.5
)

type Node struct {
	v    uint32
	item string
	ls   []*Level
}

type Level struct {
	next *Node
}

type SkipLista struct {
	hn *Node
	h  int
	c  int
}

func NewSkipList() *SkipLista {
	return &SkipLista{
		hn: NewNode(maxl, "nil", 0),
		h:  1,
		c:  0,
	}
}

func NewNode(level int, name string, val uint32) *Node {
	n := new(Node)
	n.v = val
	n.item = name
	n.ls = make([]*Level, level)
	for i := 0; i < len(n.ls); i++ {
		n.ls[i] = new(Level)
	}
	return n
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (sl *SkipLista) randomlvl() int {
	l := 1
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for r.Float64() < p && l < maxl {
		l++
	}
	return l
}

func (sl *SkipLista) add(name string) bool {

	value := hash(name)

	if value <= 0 {
		return false
	}

	update := make([]*Node, maxl)
	th := sl.hn

	for i := sl.h - 1; i >= 0; i-- {
		for th.ls[i].next != nil && th.ls[i].next.v < value {
			th = th.ls[i].next
		}
		if th.ls[i].next != nil && th.ls[i].next.v == value {
			return false
		}

		update[i] = th
	}

	level := sl.randomlvl()
	node := NewNode(level, name, value)
	if level > sl.h {
		sl.h = level
	}

	for i := 0; i < level; i++ {
		if update[i] == nil {
			sl.hn.ls[i].next = node
			continue
		}
		node.ls[i].next = update[i].ls[i].next
		update[i].ls[i].next = node
	}

	sl.c++
	return true
}

func (sl *SkipLista) find(name string) (*Node, bool) {
	value := hash(name)

	var node *Node
	th := sl.hn

	for i := sl.h - 1; i >= 0; i-- {
		for th.ls[i].next != nil && th.ls[i].next.v <= value {
			th = th.ls[i].next
		}

		if th.v == value {
			node = th
			break
		}
	}
	if node == nil {
		return nil, false
	}

	return node, true
}

func (sl *SkipLista) delete(name string) bool {
	value := hash(name)
	var node *Node
	last := make([]*Node, sl.h)
	th := sl.hn

	for i := sl.h - 1; i >= 0; i-- {
		for th.ls[i].next != nil && th.ls[i].next.v < value {
			th = th.ls[i].next
		}

		last[i] = th

		if th.ls[i].next != nil && th.ls[i].next.v == value {
			node = th.ls[i].next
		}
	}

	if node == nil {
		return false
	}

	for i := 0; i < len(node.ls); i++ {
		last[i].ls[i].next = node.ls[i].next
		node.ls[i].next = nil
	}

	for i := 0; i < len(sl.hn.ls); i++ {
		if sl.hn.ls[i].next == nil {
			sl.h = i
			break
		}
	}
	sl.c--
	return true
}
