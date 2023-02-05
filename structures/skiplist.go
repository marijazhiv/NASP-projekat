package structures

//Boris Markov SV/73-2021

import (
	"hash/fnv"
	"math/rand"
	"time"
)

const (
	maxl = 32  //max visina skipliste
	p    = 0.5 //faktor bacanja novcica
)

type Node struct {
	v         uint32 //hashirana vrednost node-a
	item      string //ime node-a
	tombstone bool   //tombstone elemenat
	ls        []*Level
}

type Level struct {
	next *Node
}

type SkipLista struct {
	hn *Node
	h  int
	c  int
}

func NewSkipList() *SkipLista { //inicijalizacija skip liste
	return &SkipLista{
		hn: NewNode(maxl, "nil", 0),
		h:  1,
		c:  0,
	}
}

func NewNode(level int, name string, val uint32) *Node { //inicijalizacija jednog node-a
	n := new(Node)
	n.v = val
	n.item = name
	n.tombstone = false
	n.ls = make([]*Level, level)
	for i := 0; i < len(n.ls); i++ {
		n.ls[i] = new(Level)
	}
	return n
}

func HashSL(s string) uint32 { //funkcija hashiranja stringa u uint32
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (sl *SkipLista) randomlvl() int { //odredjivanje nivoa node-a bacanjem novcica
	l := 1
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for r.Float64() < p && l < maxl {
		l++
	}
	return l
}

func (sl *SkipLista) add(name string) bool {

	value := HashSL(name)

	if value <= 0 {
		return false
	}

	update := make([]*Node, maxl)
	th := sl.hn

	for i := sl.h - 1; i >= 0; i-- { //generisemo mesta gde cemo ubaciti fajl
		for th.ls[i].next != nil && th.ls[i].next.v < value {
			th = th.ls[i].next
		}
		if th.ls[i].next != nil && th.ls[i].next.v == value {
			return false
		}

		update[i] = th
	}

	level := sl.randomlvl() //generisemo nivo do kog ce node skociti
	node := NewNode(level, name, value)
	if level > sl.h {
		sl.h = level
	}

	for i := 0; i < level; i++ { //na svakom nivou (do generisanog) ga zapisujemo
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
	value := HashSL(name)

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
	value := HashSL(name)
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

	node.tombstone = true
	// for i := 0; i < len(node.ls); i++ {
	// 	last[i].ls[i].next = node.ls[i].next
	// 	node.ls[i].next = nil
	// }

	// for i := 0; i < len(sl.hn.ls); i++ {
	// 	if sl.hn.ls[i].next == nil {
	// 		sl.h = i
	// 		break
	// 	}
	// }
	sl.c--
	return true
}
