package strukture

import (
	"encoding/gob"
	"hash"
	"math"
	"os"
	"time"

	"github.com/spaolacci/murmur3"
)

// Bloom filter moze da nam kaze da li element sigurno nije u skupu, ili je on mozda u skupu
type BloomFilter struct {
	M          uint          // velicina set-ova
	K          uint          // broj hash funkcija
	P          float64       // false-pozitiv (1-[1-1/m]^kn)^k ///// n je pretpostavljen broj elemenata u setu
	set        []byte        // set koji sadrzi bitove
	hash       []hash.Hash32 // hash
	time_const uint
}

func Create_BloomFilter(n uint, p float64) *BloomFilter {
	m := Formula_M_BF(int(n), p) //po formulama nalazimo m i k
	k := Formula_K_BF(int(n), m)
	hash, time := Create_Hash_BF(k) //pozivamo funkciju koja pravi hash
	bloom_filter := BloomFilter{
		m,
		k,
		p,
		make([]byte, m),
		hash,
		time} //na kraju inicijalizujemo bloom_filter objekat
	//prosledjujemo memorijsku adresu na njega
	return &bloom_filter
}

func Formula_M_BF(n int, p float64) uint {
	// m = − ( (n * ln(p)) / (ln 2)^2 )
	// formula za m, gde je n ocekivani broj elemenata u setu, a p je false-positive verovatnoca
	m := uint(math.Ceil(float64(n) * math.Abs(math.Log(p)) / math.Pow(math.Log(2), float64(2))))
	return m
}

func Formula_K_BF(n int, m uint) uint {
	// k = (m/n)*ln(2)
	// formula za k, gde je n ockivani broj elemenata u setu, m je velicina bit seta *predhodno izracunata uz Formula_M funkciju*
	k := uint(math.Ceil((float64(m) / float64(n)) * math.Log(2)))
	return k
}

func Create_Hash_BF(k uint) ([]hash.Hash32, uint) {
	var h []hash.Hash32
	time := uint(time.Now().Unix()) //razlika vremena u sekundama u trenutku kreiranja hash-a i 1. januara 1970.
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(time+1)))
	}
	return h, time
}

func Copy_Hash(k uint, time uint) []hash.Hash32 {
	//kopija jedne hash funkcije, korisi se i za count_min_sketch takodje
	//razkila od kreiranja nove je sto je vreme vec poznato, tj ne trazimo vremesku razliku ovog trenutka, vec je prosledjujemo, pravimo samo COPY-ju predhodno kreiranog hash-a
	var h []hash.Hash32
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(time+1)))
	}
	return h
}

func (bloom_filter *BloomFilter) Add_Element_BF(elem Element) { //dodavanje kljuca elementa u bloom filter
	for _, hash_func := range bloom_filter.hash { //za svaku hash funkciju iz bloom_filter-a
		i := HashIt_BF(hash_func, elem.Key, bloom_filter.M) //izracunavamo indekse u setu koje prebacujemo sa 0 na 1
		bloom_filter.set[i] = 1                             //kolizija -> *na ovom mestu je vec 1* -> nastavi dalje
	}
}

func (bloom_filter *BloomFilter) Query_BF(elem string) bool {
	for _, hash_func := range bloom_filter.hash { //za svaku hash funkciju iz bloom_filter-a
		i := HashIt_BF(hash_func, elem, bloom_filter.M) //izracunavamo indekse u setu radi provere
		if bloom_filter.set[i] != 1 {                   //ako je vrednost u setu 0
			return false //element SIGURNO *100%* nije u setu
		}
	}
	return true //ako funkcija vrati true, znaci da su na svim pozicijama pronadjene 1, ali to ne garantuje prisutnost elementa *KOLIZIJA*
}

func HashIt_BF(hash_func hash.Hash32, elem string, m uint) uint32 {
	_, err := hash_func.Write([]byte(elem)) //upisuje elem *u bajtovima* u hash
	if err != nil {
		panic(err)
	}
	i := hash_func.Sum32() % uint32(m) // i = h(elem) % m
	hash_func.Reset()                  // resetujemo hash
	return i                           // vracamo i, tj index seta na kom se nalazi element
}

func Write_BloomFilter(filename string, bloom_filter *BloomFilter) {
	//upisivanje bloom_filtera u file, pod nazivom koji se nalazi u parametrima funkcije
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//da bismo upisali bloom_filter u file neophodno je prevodjenje objekta u niz bajtova
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bloom_filter)
	if err != nil {
		panic(err)
	}
}

func Read_BloomFilter(filename string) (bloom_filter *BloomFilter) {
	//citanje bloom_filtera iz file koji se prosledjuje kao parametar prilikom poziva funkcije
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//da bismo iscitali bloom_filter u file neophodno je prevodjenje niza bajtova u objekat bloom_filter-a
	decoder := gob.NewDecoder(file)
	bloom_filter = new(BloomFilter)
	_, err = file.Seek(0, 0) //seek na pocetak file-a
	if err != nil {
		return nil
	}

	for {
		err = decoder.Decode(bloom_filter)
		if err != nil {
			break
		}
	}
	bloom_filter.hash = Copy_Hash(bloom_filter.K, bloom_filter.time_const)
	return //bloom_filter je uspesno iscitan iz file-a i kreiran
}
