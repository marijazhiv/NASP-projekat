// Dusica Trbovic, SV 42/2021
package structures

import (
	"bytes"
	"encoding/gob"
	"hash"
	"math"
	"time"

	"github.com/spaolacci/murmur3"
)

// struktura koja sluzi kao tabela ucestalosti dogadjaja u stream-u podataka
type CountMinSketch struct {
	M          uint          // broj kolona *ne biraju se nasumicno*
	K          uint          // broj redova, tj hash funkcija *ne biraju se nasumicno*
	E          float64       // procena ucestalosti dogadjaja
	D          float64       // d = ln (1/delta), tacnost koju celimo da postignemo u pronalazenju ucestalosti
	set        [][]int       //set koji sadrzi bitove
	hash       []hash.Hash32 // hash
	time_const uint
}

func Create_CountMinSketch(p float64, d float64) *CountMinSketch {
	m := Formula_M_CMS(p) //po formuli pronalazimo m i k
	k := Formula_K_CMS(d)
	hash, time := Create_Hash_CMS(k) //pozivamo funkciju koja pravi hash
	set := make([][]int, k, k)
	for i := range set {
		set[i] = make([]int, m, m)
	}
	//inicijalno svi elementi unutar count min sketch-a su postavljeni na 0,
	//CMS[i,j] = 0 za svako i,j, gde su i ={0,1..k}, j ={0,1...m}

	count_min_sketch := CountMinSketch{
		m, k, p, d, set, hash, time} //na kraju inicijalizujemo count_min_sketch objekat

	//prosledjujemo memorijsku adresu na njega
	return &count_min_sketch
}

func Formula_M_CMS(epsilon float64) uint {
	//racunamo broj kolona koje se prave
	//m = E/epsilon, ovo je zapravo w *prezentacije*, preciznost
	m := uint(math.Ceil(math.E / epsilon))
	return m
}

func Formula_K_CMS(delta float64) uint {
	//racunamo broj hash funkcija koje cemo praviti, tj broj redova
	//k = ln (1/delta), ovo je zapravo d *prezentacije*, tacnost
	k := uint(math.Ceil(math.Log(math.E / delta)))
	return k
}

func Create_Hash_CMS(k uint) ([]hash.Hash32, uint) {
	var h []hash.Hash32
	time := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(time+1)))
	}
	return h, time
}

func (count_min_sketch *CountMinSketch) Add_Element_CMS(elem string) {
	//dodajemo element na trenutan count_min_sketch tako sto:
	//elem propustamo kroz svaku hash funkciju hi = {1,2...k}
	for i, hash_func := range count_min_sketch.hash {
		j := HashIt_CMS(hash_func, elem, count_min_sketch.M) // j = hi(elem) % m
		count_min_sketch.set[i][j] += 1                      //na preseku reda i kolone vrednost povecavamo za 1
	}
}

func (count_min_sketch *CountMinSketch) Query_CMS(elem string) int {
	//Nalazimo minimum iz niza koji smo napravili pustajuci *elem* kroz svaku hash funkciju
	values := make([]int, count_min_sketch.K, count_min_sketch.K) //values je niz, sadrzi sve vrednosti koje dobijemo kada *elem* propustimo kroz sve hash-eve, duzine je k -> *broj hash funkcija*
	for i, hash_func := range count_min_sketch.hash {             //za svaku hash funkciju
		j := HashIt_CMS(hash_func, elem, count_min_sketch.M)
		values[i] = count_min_sketch.set[i][j] //upisujemo vrednost koja se nalazi na toj poziciji u cms-u
	}
	min := values[0]               //vrednost min je prvi clan niza, a zatim primenjujemo algoritam
	for _, value := range values { //za svaki value
		if value < min { //ako je value na kom smo trenutno pozicionirani manji od trenutnog minimuma, imamo novi minimum
			min = value
		}
	}
	return min //povratna vrednost je pronadjena minimalna vrendost
}

func HashIt_CMS(hash_func hash.Hash32, elem string, m uint) uint32 {
	_, err := hash_func.Write([]byte(elem)) //upisuje elem *u bajtovima* u hash
	if err != nil {
		panic(err)
	}
	j := hash_func.Sum32() % uint32(m) // j = h(elem) % m
	hash_func.Reset()                  //resetujemo hash
	return j                           //vracamo rezultat kolone j
}

func (count_min_sketch *CountMinSketch) Serialize_CMS() []byte {
	//prevodjenje count_min_sketch objekta u niz bajtova
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(count_min_sketch)
	return buffer.Bytes() //povratna vrednost je niz bajtova koji smo dobili nakon prevodjenja
}

func Deserialize_CMS(data []byte) *CountMinSketch {
	//prevodjenje niza bajtova u count_min_sketch objekat
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	count_min_sketch := new(CountMinSketch)
	for {
		err := decoder.Decode(count_min_sketch)
		if err != nil {
			break
		}
	}
	count_min_sketch.hash = CopyHashFunctions(count_min_sketch.K, count_min_sketch.time_const)
	return count_min_sketch //povratna vrednost je count_min_sketch objekat
}
