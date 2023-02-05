package structures

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
)

//na osnovu kljuca (iz index fajla) u summary vratiti poziciju na kojoj se nalazi
func FindSummary(key, filename string) (nadjen bool, offset int64) {
	nadjen = false   //na pocetku postavimo da kljuc nije pronadjen
	offset = int64(8)  //pozicija kljuca u Index fajlu

	file, err := os.Open(filename)
	if err != nil {
		panic(err)   //otvorimo fajl
	}
	defer file.Close()

	reader := bufio.NewReader(file)  //kreiramo citac

	bytes := make([]byte, 8)   //napravimo niz od 8 bajtova (iz fajla)
	_, err = reader.Read(bytes)  //citamo ih
	if err != nil {
		panic(err)    //greska
	}
	fileLen := binary.LittleEndian.Uint64(bytes)  // //pretvara ga iz niza bajtova u unsigned int 64 binarno
//duzina fajla (procitana tipa iz headera- tu pise kolika je duzina fajla, stane u 8B)

	//granice index fajla- prvi i poslednji kljuc

	//start Key
	
	bytes = make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	keyLen := binary.LittleEndian.Uint64(bytes)  //duzina kljuca (procitam je na isti nacin kao fileLen)

	bytes = make([]byte, keyLen)  //
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	startKey := string(bytes[:]) //bajtove kljuca pretvorim u string

	if key < startKey {    //ako je trazeni kljuc manji od startne granice indexa fajla-> false  (izvan opsega)
		return false, 0
	}

	//end Key
	bytes = make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	keyLen = binary.LittleEndian.Uint64(bytes)  //duzina kljuca

	bytes = make([]byte, keyLen)  
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	endKey := string(bytes[:])  //string od bajtova krajnjeg kljuca

	if key > endKey {
		return false, 0    //ako je trazeni kljuc veci od krajnjeg kljuca-> false (izvan opsega)
	}

	nadjen = true  //nadjen sigurno jer je u opsegu
	var i uint64
	for i = 0; i < fileLen-2; i++ {  //od pocetka fajla do kraja bez poslednja dva elementa (start i end key)
		good := false   //da li je i dalje u opsegu
		bytes := make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		keyLen := binary.LittleEndian.Uint64(bytes)  //duzina i-tog kljuca

		bytes = make([]byte, keyLen)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		nodeKey := string(bytes[:])   //nodekey

		if nodeKey <= key {
			good = true   //U opsegu je u sstable-u!
		}
//posle kljuca citamo mu poziciju tj offset
		bytes = make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		newOffset := binary.LittleEndian.Uint64(bytes)

		if good {
			offset = int64(newOffset)   //vraca nam offset tog kljuca koji nam -priblizno-odgovara
		} else if !good {
			break      
		}
	}
	return
}

func WriteSummary(keys []string, offsets []uint, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		return               //kreiramo fajl
	}
	defer file.Close()

	writer := bufio.NewWriter(file)   //kreiramo upisivac

	fileLen := uint64(len(keys))  //duzina fajla--> koliko ima kljuceva ukupno
	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, fileLen)  //to smestimo u niz od 8B
	_, err = writer.Write(bytesLen)
	if err != nil {
		log.Fatal(err)
	}

	for i := range keys {
		key := keys[i]  //idemo kljuc po kljuc (odnosno, vrednost kljuca)
		offset := offsets[i]  //pozicija odredjenig kljuca (i-tog)

		bytes := []byte(key)  //kljuc smestamo u niz bajtova

		keyLen := uint64(len(bytes))  //duzina kljuca-> unsigned int od 8 bajtova
		bytesLen := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytesLen, keyLen)  //upis duzine u niz od 8 bajtova
		_, err := writer.Write(bytesLen) //upis te duzine kljuca
		if err != nil {
			log.Fatal(err)
		}

		_, err = writer.Write(bytes) //?
		if err != nil {
			log.Fatal(err)
		}

		if i >= 2 {
			bytes = make([]byte, 8)   //u 8B upisuje i poziciju kljuca usstable index-u
			binary.LittleEndian.PutUint64(bytes, uint64(offset))
			_, err = writer.Write(bytes)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = writer.Flush()
		if err != nil {
			return
		}
	}
}
