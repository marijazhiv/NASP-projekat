package structures

// Boris Markov SV73/2021
import (
	"bufio"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
)

type Index interface { //
	Add()                  //dodavanje entry u indeks strukturu
	Find()                 //pretraga
	Write(filename string) //ispis u fajl
}

type SSindex struct {
	filename   string   //ime fajla
	Offset     uint     //ofset zapisanih podataka
	KeySize    uint     //velicina kljuca
	DataKeys   []string //lista kljuceva
	DataOffset []uint   //lista offseta
}

func CreateIndex(keys []string, offsets []uint, filename string) *SSindex { //inicijalizacija Index strukture
	index := SSindex{filename: filename}
	for i, key := range keys {
		index.Add(key, offsets[i]) //appendujemo sve podatke u Indeks strukturu
	}
	return &index
}

func (index *SSindex) Add(key string, offset uint) {
	index.DataKeys = append(index.DataKeys, key)
	index.DataOffset = append(index.DataOffset, offset)
}

func FindIndex(key string, offset int64, filename string) (ok bool, dataOffset int64) {
	ok = false
	dataOffset = 0 //inicijalne vrednosti su negativne, dokle god ne potvrdimo pretragu

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close() //na kraju zatvaramo fajl

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen := binary.LittleEndian.Uint64(bytes) //duzina fajla? belezimo

	_, err = file.Seek(offset, 0)
	if err != nil {
		return false, 0
	} //proveravamo da li je fajl prazan

	reader = bufio.NewReader(file)

	var i uint64
	for i = 0; i < fileLen; i++ { //idemo redom po kljucevima dok ne nadjemo onaj koji nam treba
		bytes := make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		keyLen := binary.LittleEndian.Uint64(bytes) //transformisemo kljuc u uint64

		bytes = make([]byte, keyLen)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		nodeKey := string(bytes[:])

		if nodeKey == key { //da li smo nasli kljuc?
			ok = true
		} else if nodeKey > key { //izasli smo iz opsega, neuspesna pretraga (niz je sortiran!)
			return false, 0
		}

		bytes = make([]byte, 8) //nasli smo ga, uzimamo offset
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		newOffset := binary.LittleEndian.Uint64(bytes)

		if ok { //ako smo nasli, napustamo petlju
			dataOffset = int64(newOffset)
			break
		}
	}
	return
}

func (index *SSindex) Write() (keys []string, offsets []uint) { //ispis u fajl
	currOffset := uint(0)
	file, err := os.Create(index.filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	bytesLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLength, uint64(len(index.DataKeys))) //enkriptujemo podatke u bajtove
	bytesWritten, err := writer.Write(bytesLength)
	if err != nil {
		log.Fatal(err)
	}

	currOffset += uint(bytesWritten)
	err = writer.Flush()
	if err != nil {
		return
	}

	rangeKeys := make([]string, 0)
	rangeOffsets := make([]uint, 0)
	sampleKeys := make([]string, 0)
	sampleOffsets := make([]uint, 0)

	for i := range index.DataKeys { //iteriramo podatke i zapisujemo ih u fajl, takodje belezimo sta smo sve ispisali i vracamo kao povratnu vrednost
		key := index.DataKeys[i]
		offset := index.DataOffset[i]

		if i == 0 || i == (len(index.DataKeys)-1) {
			rangeKeys = append(rangeKeys, key)
			rangeOffsets = append(rangeOffsets, currOffset)
		} else if rand.Intn(100) > 50 {
			sampleKeys = append(sampleKeys, key)
			sampleOffsets = append(sampleOffsets, currOffset)
		}

		bytes := []byte(key)

		keyLength := uint64(len(bytes))
		bytesLength := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytesLength, keyLength)
		bytesWritten, err := writer.Write(bytesLength)
		if err != nil {
			log.Fatal(err)
		}
		currOffset += uint(bytesWritten)

		bytesWritten, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		currOffset += uint(bytesWritten)

		bytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(offset))
		bytesWritten, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		currOffset += uint(bytesWritten)
	}
	err = writer.Flush()
	if err != nil {
		return
	}
	keys = append(rangeKeys, sampleKeys...)
	offsets = append(rangeOffsets, sampleOffsets...)
	return
}
