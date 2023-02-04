package structures

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/bits"
)

type hyperLogLog struct {
	registers []int //niz baketa
	m         uint  //broj baketa
	p         uint  //preciznost
}

func NewHLL(m uint) hyperLogLog { //inicijalizacija
	return hyperLogLog{
		registers: make([]int, m),
		m:         m,
		p:         uint(math.Ceil(math.Log2(float64(m)))),
	}
}

func convertToUint32(s string) uint32 { //funkcija hashiranja stringa u uint32
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (h hyperLogLog) Add(data string) hyperLogLog {
	dummy := convertToUint32(data) //convertujemo string u uint32, da bi imali istu duzinu podataka
	b := make([]byte, 4)           //ubacujemo ga u 4 bajta
	binary.LittleEndian.PutUint32(b, dummy)
	x := createHash(b)
	k := 32 - h.p //prvih b bita (zavisi od preciznosti)
	r := leftmostActiveBit(x << h.p)
	j := x >> uint(k)
	if r > h.registers[j] { //dodeljujemo broj nula registru koji je odredjen prvim p bita
		h.registers[j] = r
	}
	return h
}

func (hll *hyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.registers {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (h hyperLogLog) Count() uint64 {
	sum := 0.
	for _, v := range h.registers {
		sum += math.Pow(math.Pow(2, float64(v)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(h.m))
	estimation := alpha * math.Pow(float64(h.m), 2.0) / sum
	emptyRegs := h.emptyCount()
	if estimation <= 2.5*float64(h.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(h.m) * math.Log(float64(h.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}

	return uint64(estimation)
}

func leftmostActiveBit(x uint32) int { //trazi duzinu niza nula sa kraja broja
	return 1 + bits.LeadingZeros32(x)
}

func createHash(stream []byte) uint32 { //pretvara vrednost u 32bitni hash
	h := fnv.New32()
	h.Write(stream)
	sum := h.Sum32()
	h.Reset()
	return sum
}
