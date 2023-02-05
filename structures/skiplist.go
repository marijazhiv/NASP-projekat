package structures

//Boris Markov SV/73-2021

import (
	"math/rand"
	"time"
)

type SkipLista struct {
	maxh int      //maximalna visina
	hn   *Element //glava skipliste
	h    int      //trenutna visina
	c    int      //duzina skipliste
}

func NewSkipList(maxh int) *SkipLista { //inicijalizacija skip liste
	bytes := []byte("head")
	crc := CRC32(bytes)
	root := Element{"head", nil, make([]*Element, maxh+1), time.Now().String(),
		false, crc}
	skiplist := SkipLista{maxh, &root, 1, 1}
	return &skiplist
}

func (sl *SkipLista) randomlvl() int { //odredjivanje nivoa node-a bacanjem novcica
	l := 0
	for ; rand.Int31n(2) == 1; l++ {
		if l > sl.h {
			sl.h = l
			return l
		}
	}
	return l
}

func (sl *SkipLista) Add(key string, value []byte, tombstone bool) *Element {
	level := sl.randomlvl()
	bytes := []byte(key)
	crc := CRC32(bytes)
	node := &Element{key, value, make([]*Element, level+1), time.Now().String(), tombstone, crc} //generisemo node sa vrednostima
	for i := sl.h - 1; i >= 0; i-- {
		current := sl.hn
		next := current.Next[i]
		for next != nil {
			if next == nil || next.Key > key { //ako je naisao na nekog sa vecim kljucem, spustamo se nivo
				break
			}
			current = next
			next = current.Next[i]
		}
		if i <= level { //nije nasao veci? spustamo se nivo
			sl.c++
			node.Next[i] = next
			current.Next[i] = node
		}
	}
	return node
}

func (sl *SkipLista) Delete(key string) *Element {

	curr := sl.hn
	for i := sl.h - 1; i >= 0; i-- { //idemo od najviseg ka nizem nivou
		next := curr.Next[i]
		for next != nil {
			curr = next
			next = curr.Next[i]
			if next == nil || curr.Key > key {
				break
			}
			if curr.Key == key { //nadjen prepravljamo da bude logicki obrisan (i menjamo timestamp)
				curr.Tombstone = true
				curr.Timestamp = time.Now().String()
				tmp := curr
				curr = curr.Next[i]
				return tmp
			}
		}
	}
	return nil
}

func (sl *SkipLista) Find(key string) *Element {

	current := sl.hn
	for i := sl.h - 1; i >= 0; i-- {
		next := current.Next[i]
		for next != nil {
			current = next
			next = current.Next[i]
			if current.Key == key { //nije nam bitan na kom smo nivou, nasli smo trazeni node
				return current
			}
			if next == nil || current.Key > key {
				break
			}
		}
	}
	return nil
}
