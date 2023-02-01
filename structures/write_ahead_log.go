//Dusica Trbovic, SV 42/2021
package structures

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// konstantne promenljive koje odredjuju strukturu WAL-a, velicina je izrazena u bajtima
const (
	WAL_path  = "./data/write_ahead_log/"
	CRC       = 4
	TimeStamp = 16 //vreme kada je izvrsena operacija
	Tombstone = 1  //govori nam vise o operaciji, da li je brisanje ili dodavanje
	KeySize   = 8  //velicina kljuca
	ValueSize = 8  //velicina vrednosti koja se nalazi uz kljuc

	SEGMENT_CAPACITY = 50
	LOW_WATER_MARK   = 0
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE((data)) //ChecksumIEEE returns the CRC-32 checksum of data using the IEEE polynomial
}

type Segment struct { //jedan segment u WAL-u
	index    uint64
	data     []byte
	size     uint64
	capacity uint64
}

func (s *Segment) Index() uint64 { //dobavljanje index-a iz segmenta
	return s.index
}

func (s *Segment) Data() []byte { //dobavljanje podataka iz segmenta
	return s.data
}

func (s *Segment) addData(elemData []byte) int { //dodavanje podataka, moze se odviti uspesno (return -1) ili fail-ovati (return i)
	for i := 0; i < len(elemData); i++ {
		if s.size >= s.capacity { //ako se desi da je size veci nego dozvoljena velicina, zaustavlja se funkcija
			return i
		}
		s.data = append(s.data, elemData[i]) // na niz s.data dodaje se elemData[i], i prelazi se na sledeci element iz prosledjenog niza
		s.size++
	}
	return -1 //ako je uspesno dodat ceo prosledjeni niz, funkcija vraca -1
}

func (s *Segment) Dump(walPath string) { // cuvamo podatke na nekoj novoj lokaciji, preventiva radi sprecavanja gubitka podataka

	path := walPath + "wal" + strconv.FormatUint(s.index, 10) + ".log" //putanja ka fajlu
	nwf, _ := os.Create(path)                                          //kreiramo novi fajl
	err := nwf.Close()                                                 //zatvaramo kreirani fajl
	if err != nil {
		fmt.Println(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY, 0666) //otvaramo fajl na datoj putanji, u write-only rezimu
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close() //zatvaranje fajla se 'odlaze', tj izvrsice se cim se pozove bilo koji return
	bufferedWriter := bufio.NewWriter(file)
	err = bufferedWriter.Flush() //'forsiramo' bufer da upise podatke u fajl
	if err != nil {
		return
	}

	_, err = bufferedWriter.Write(s.data) //upisujemo sve podatke iz segmenta u buffer
	err = bufferedWriter.Flush()          //'forsiramo' buffer da upise podatke u fajl
	if err != nil {
		return
	}

	if err != nil {
		log.Fatal(err)
	}
}

type Wal struct { //struktura WAL-a
	path           string
	lowWaterMark   uint
	segmentSize    uint
	segmentsNames  map[uint64]string
	segments       []*Segment
	currentSegment *Segment
}

func (w *Wal) CurrentSegment() *Segment { //vraca trenutni segment
	return w.currentSegment
}

func (w *Wal) Path() string { //vraca putanju na kojoj se nalaze WAL datoteke
	return w.path
}

func CreateWal(path string) *Wal { //kreiranje WAL strukture
	wal := Wal{
		path,
		LOW_WATER_MARK,
		SEGMENT_CAPACITY,
		make(map[uint64]string),
		make([]*Segment, 0), &Segment{
			index:    0,
			data:     nil,
			size:     0,
			capacity: SEGMENT_CAPACITY,
		}}

	return &wal
}

func (w *Wal) Dump() { //kreiramo kopiju podataka i cuvamo odvojeno od memorije, radi preventive od gubljenja podataka
	w.currentSegment.Dump(w.path)
	w.segmentsNames[w.currentSegment.index] = "wal" + strconv.FormatUint(w.currentSegment.index, 10) + ".log"
}

func (w *Wal) NewSegment() { //kreiramo novi segment
	newSegm := Segment{
		index:    w.currentSegment.index + 1,
		data:     make([]byte, 0, SEGMENT_CAPACITY),
		size:     0,
		capacity: w.currentSegment.capacity,
	}

	//updatujemo podatke koji se trenutno nalaze u WAL-u
	w.Dump()
	w.segments = append(w.segments, &newSegm)
	w.currentSegment = &newSegm
	w.Dump()
}

type Element struct { //struktura element
	Key       string
	Value     []byte
	Next      []*Element
	Timestamp string
	Tombstone bool
	Checksum  uint32
}

func (w *Wal) Put(elem *Element) bool { //put funkcija, dodaje novi element u WAL
	//little endian, od vecih bajta ka manjim, kovertujemo binarni tekst u int (32/64, zavisi kako je naznaceno)
	crc := make([]byte, CRC)
	binary.LittleEndian.PutUint32(crc, elem.Checksum)
	timestamp := make([]byte, TimeStamp)
	binary.LittleEndian.PutUint64(timestamp, uint64(time.Now().Unix()))
	tombstone := make([]byte, Tombstone)
	switch elem.Tombstone {
	case true:
		tombstone = []byte{1}
	case false:
		tombstone = []byte{0}
	}
	key_size := make([]byte, KeySize)
	value_size := make([]byte, ValueSize)
	binary.LittleEndian.PutUint64(key_size, uint64(len(elem.Key)))
	binary.LittleEndian.PutUint64(value_size, uint64(len(elem.Value)))

	key := []byte(elem.Key)
	value := elem.Value

	//kreiramo novi slice, jer nismo u mogucnosti da appendujemo dva niza jedan na drugi, samo dva slice objekta, tako da dodajemo bajtove na bajtove..
	elemData := []byte{}
	elemData = append(elemData, crc...)
	elemData = append(elemData, timestamp...)
	elemData = append(elemData, tombstone...)
	elemData = append(elemData, key_size...)
	elemData = append(elemData, value_size...)
	elemData = append(elemData, key...)
	elemData = append(elemData, value...)

	start := 0
	offset := 0
	for offset >= 0 { //ako je podataka uspesno dodat u WAL petlja se zavrsava
		offset = w.CurrentSegment().addData(elemData[start:]) // dodajemo novi segment na trenutni WAL
		if offset != -1 {                                     // ako se ne doda uspesno kreiramo novi segment i nastavljamo sa for petljom
			w.NewSegment()
			start = start + offset //start pomeramo za offset, tj ono sto smo dobili kao povratnu vrednost prilikom dodavanja podataka u WAL
		}
	}

	return true
}

func (wal *Wal) ReadWal(path string) { //citanje WAL-a
	files, err := ioutil.ReadDir(WAL_path) //lista tekstova iz razlicitih fajlova koji se nalaze u WAL folderu
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < len(files); i++ { //za svaki fajl posebno prolazimo petlju
		index_s := strings.Split(files[i].Name(), "wal")[1] //odvajamo index iz imena txt fajla (oni su u formatu wal-index.txt)
		index_s = strings.Split(index_s, ".log")[0]         //brisemo i kraj sa indeksa, tako da nam ostaje samo index
		indexx, err := strconv.ParseUint(index_s, 10, 64)   //prebacujemo indeks koji smo dobili u dekadni broj
		if err != nil {
			fmt.Println(err)
		}
		wal.segmentsNames[indexx] = files[i].Name()
	}

	max := uint64(0)
	for key := range wal.segmentsNames { //trazimo maximalnu vrednost kljuca od svih segmenata koji se nalaze u WAL-u
		if max < key {
			max = key
		}
	}
	index := max                        //pravimo novi index, onaj koji smo nasli da je maximalan
	current := wal.segmentsNames[index] // trenutan segment pozicioniramo na onaj sa najvecim indeksom

	file, err := os.Open(path + current) //otvaramo file "trenutnog" segmenta
	if err != nil {
		fmt.Println(err)
	}

	err = file.Close() //zatvaramo trenutni file
	if err != nil {
		fmt.Println(err)
	}

	bufferedReader := bufio.NewReader(file) //pravimo buffer i ucitavamo podatke iz fajla
	info, err := os.Stat(file.Name()) //podaci o file po imenu file.Name()

	if err != nil {
		fmt.Println(err)
	}
	num_of_bytes := info.Size() //proveravamo velicinu datoteke (velicina u bajtovima)

	bytes := make([]byte, num_of_bytes) //pravimo novu promenljivu tipa array byte, velicine koja je procitana iz file-a
	_, err = bufferedReader.Read(bytes)

	currentSegment := Segment{ //trenutni segment, sve podatke inicijalno postavljamo na "nulu"
		index:    index,
		data:     nil,
		size:     0,
		capacity: SEGMENT_CAPACITY,
	}

	currentSegment.addData(bytes) //dodajemo podatke koje smo predhodno iscitali iz file-a
	wal.currentSegment = &currentSegment //pokazivac "currentSegment" iz WAL-a postavljamo na trenutni segment koji smo kreirali par linija iznad
	wal.segments = append(wal.segments, &currentSegment) //na listu segmenata koji se nalaze u WAL-u dodajemo i trenutni

	err = file.Close() //zatvaramo file
	if err != nil {
		fmt.Println(err)
	}
}

func (w *Wal) RemoveSegments() { //brisanje segmenta iz WAL-a
	w.lowWaterMark = uint(w.currentSegment.index - 2)
	for index, value := range w.segmentsNames {
		index2 := uint(index)
		if index2 <= w.lowWaterMark {
			err := os.Remove(WAL_path + value)
			delete(w.segmentsNames, index)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}
