package structures

//Dusica Trbovic, SV 42/2021

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
	M         uint          // velicina Set-ova
	K         uint          // broj hash funkcija
	P         float64       // false-pozitiv (1-[1-1/m]^kn)^k ///// n je pretpostavljen broj elemenata u Setu
	Set       []byte        // Set koji sadrzi bitove
	hashs     []hash.Hash32 // hash
	TimeConst uint
}

func CreateBloomFilter(n uint, p float64) *BloomFilter {
	m := CalculateM(int(n), p) //po formulama nalazimo m i k
	k := CalculateK(int(n), m)
	hashs, tc := CreateHashFunctions(k)                    //pozivamo funkciju koja pravi hash
	bf := BloomFilter{m, k, p, make([]byte, m), hashs, tc} //na kraju inicijalizujemo bloom_filter objekat
	//prosledjujemo memorijsku adresu na njega
	return &bf
}

func (bf *BloomFilter) Add(elem Element) { //dodavanje kljuca elementa u bloom filter
	for _, hashF := range bf.hashs { //za svaku hash funkciju iz bloom_filter-a
		i := HashIt(hashF, elem.Key, bf.M) //izracunavamo indekse u Setu koje prebacujemo sa 0 na 1
		bf.Set[i] = 1                      //kolizija -> *na ovom mestu je vec 1* -> nastavi dalje
	}
}

func (bf *BloomFilter) Query(elem string) bool {
	for _, hashF := range bf.hashs { //za svaku hash funkciju iz bloom_filter-a
		i := HashIt(hashF, elem, bf.M) //izracunavamo indekse u Setu radi provere
		if bf.Set[i] != 1 {            //ako je vrednost u Setu 0
			return false //element SIGURNO *100%* nije u Setu
		}
	}
	return true //ako funkcija vrati true, znaci da su na svim pozicijama pronadjene 1, ali to ne garantuje prisutnost elementa *KOLIZIJA*
}

func HashIt(hashF hash.Hash32, elem string, m uint) uint32 {
	_, err := hashF.Write([]byte(elem)) //upisuje elem *u bajtovima* u hash
	if err != nil {
		panic(err)
	}
	i := hashF.Sum32() % uint32(m) // i = h(elem) % m
	hashF.Reset()
	return i // vracamo i, tj index Seta na kom se nalazi element
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	// m = − ( (n * ln(p)) / (ln 2)^2 )
	// formula za m, gde je n ocekivani broj elemenata u Setu, a p je false-positive verovatnoca
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	// k = (m/n)*ln(2)
	// formula za k, gde je n ockivani broj elemenata u Setu, m je velicina bit Seta *predhodno izracunata uz Formula_M funkciju*
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint) ([]hash.Hash32, uint) {
	var h []hash.Hash32
	ts := uint(time.Now().Unix()) //razlika vremena u sekundama u trenutku kreiranja hash-a i 1. januara 1970.
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h, ts
}

func CopyHashFunctions(k uint, tc uint) []hash.Hash32 {
	//kopija jedne hash funkcije, korisi se i za count_min_sketch takodje
	//razkila od kreiranja nove je sto je vreme vec poznato, tj ne trazimo vremesku razliku ovog trenutka, vec je prosledjujemo, pravimo samo COPY-ju predhodno kreiranog hash-a
	var h []hash.Hash32
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(tc+1)))
	}
	return h
}

func writeBloomFilter(filename string, bf *BloomFilter) {
	//upisivanje bloom_filtera u file, pod nazivom koji se nalazi u parametrima funkcije
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	//da bismo upisali bloom_filter u file neophodno je prevodjenje objekta u niz bajtova
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bf)
	if err != nil {
		panic(err)
	}
}

func readBloomFilter(filename string) (bf *BloomFilter) {
	//citanje bloom_filtera iz file koji se prosledjuje kao parametar prilikom poziva funkcije
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	//da bismo iscitali bloom_filter u file neophodno je prevodjenje niza bajtova u objekat bloom_filter-a
	decoder := gob.NewDecoder(file)
	bf = new(BloomFilter)
	_, err = file.Seek(0, 0) //seek na pocetak file-a
	if err != nil {
		return nil
	}

	for {
		err = decoder.Decode(bf)
		if err != nil {
			//fmt.Println(err)
			break
		}
		//fmt.Println(*bf)
	}
	bf.hashs = CopyHashFunctions(bf.K, bf.TimeConst)
	return //bloom_filter je uspesno iscitan iz file-a i kreiran
}
