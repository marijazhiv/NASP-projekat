package structures

import (
    "bufio"
    "encoding/binary"
    "errors"
    "io"
    "log"
    "os"
    "strconv"
    "strings"
)

type Table interface {
    Add()
    Find()
    Write()
}

type SSTable struct {
    generalFilename string    //nazivi svih fajlova za jedan SStable
    SSTableFilename string
    indexFilename   string
    summaryFilename string
    filterFilename  string
}

//kada se MemTable napuni, zapisuje se na disk i formira se SStable
func CreateSStable(data MemTable, filename string) (table *SSTable) {
    //prilikom kreiranja se koristi format imenovanja usertable-GENERATION-ELEMENT.[db/txt]
    generalFilename := "data/sstable/usertable-data-ic-" + filename + "-lev1-"
    table = &SSTable{generalFilename, generalFilename + "Data.db", generalFilename + "Index.db",
        generalFilename + "Summary.db", generalFilename + "Filter.gob"}
	//n i p po njima se racuna m i k
    filter := Create_BloomFilter(data.Size(), 2)    //bloomfilter za skup kljuceva  //ocekivana velicina podataka, vrednost false-positive verovatnoce koju dopustamo u sistemu; p=2
    keys := make([]string, 0)             
    offset := make([]uint, 0)       //pozicija u sstable-u
    values := make([][]byte, 0)
    currentOffset := uint(0)
    file, err := os.Create(table.SSTableFilename)
    if err != nil {
        log.Fatal(err)          //pravi fajl, prosledjen parametar Filename
    }
    defer file.Close()

    writer := bufio.NewWriter(file)    //citac

    // file length
    bytesLen := make([]byte, 8)  //niz od 8 bajtova
    binary.LittleEndian.PutUint64(bytesLen, uint64(data.Size()))   //najmanji bit se cuva na prvoj adresi //na tih 8 bajtova belezimo kolicinu podataka
    bytesWritten, err := writer.Write(bytesLen)        //prvih 8 bajta- zaglavlje fajla; samo ih zapise
    currentOffset += uint(bytesWritten)    //povecavamo poziciju gde se nalazimo
    if err != nil {
        log.Fatal(err)  
    }

    err = writer.Flush()
    if err != nil {
        return
    }

    // We need to check if data is sorted
    for node := data.data.hn.ls[0].Next; node != nil; node = node.ls[0].Next {  //
        key := node.item
        value := node.v
        keys = append(keys, key)
        offset = append(offset, currentOffset)
        values = append(values, value)

        filter.Add_Element_BF(*node)   //dodaje sve elemente u filter
        //crc
        crc := CRC32(value)   //vraca string duzine 8 (to je tekstualni prikaz hexdec vrednosti 32-bitnog binarnog niza)
        crcBytes := make([]byte, 4)
        binary.LittleEndian.PutUint32(crcBytes, crc)   //tu crc vrednost smestamo u niz od 4B
        bytesWritten, err := writer.Write(crcBytes) //zapisemo
        currentOffset += uint(bytesWritten)
        if err != nil {      //povecamo trenutnu poziciju
            return
        }

        //Timestamp
        timestamp := node.timestamp  //
        timestampBytes := make([]byte, 16)  //pravimo niz od 16B
        copy(timestampBytes, timestamp)  //vrednost timestamp kopiramo u niz od 16 bajta  (nema podrzano PutUnit128)

        bytesWritten, err = writer.Write(timestampBytes)  //zapisemo
        if err != nil {
            log.Fatal(err)
        }
        currentOffset += uint(bytesWritten)

        //Tombstone---> ako je podatak obrisan (status polje- 0 ili 1)
        tombstone := node.tombstone
        tombstoneInt := uint8(0)  //postavi ga na nulu
        if tombstone {
            tombstoneInt = 1     //ako je obrisan on je 1
        }

        err = writer.WriteByte(tombstoneInt)   //zapise
        currentOffset += 1  //poveca trenutnu poziciju za 1
        if err != nil {
            return
        }
//key size (8B)
        keyBytes := []byte(key)  //cuva sve kljuceve u niz bajtova

        keyLen := uint64(len(keyBytes))  //velicina niza kljuceva
        keyLenBytes := make([]byte, 8)  //pravi niz od 8B
        binary.LittleEndian.PutUint64(keyLenBytes, keyLen)  //u taj niz od 8B smesta tu vrednost
        bytesWritten, err = writer.Write(keyLenBytes)  //zapise je
        if err != nil {
            log.Fatal(err)
        }
        currentOffset += uint(bytesWritten)  //poveca trenutnu poziciju za broj zapisanih bajtova
//value size (8B)---> sve isto samo cuvamo duzinu Value podataka
        valueLen := uint64(len(value))
        valueLenBytes := make([]byte, 8)
        binary.LittleEndian.PutUint64(valueLenBytes, valueLen)
        bytesWritten, err = writer.Write(valueLenBytes)
        if err != nil {
            log.Fatal(err)
        }
        currentOffset += uint(bytesWritten)   //poveca trenutnu poziciju za broj zapisanih bajtova
//key
        bytesWritten, err = writer.Write(keyBytes)  //zapise kljuceve
        if err != nil {
            log.Fatal(err)
        }
        currentOffset += uint(bytesWritten)
//value
        bytesWritten, err = writer.Write(value)   //Zapise vrednosti
        if err != nil {
            return
        }
        currentOffset += uint(bytesWritten)

        err = writer.Flush()
        if err != nil {
            return
        }
    }
    //kreiranje zadatih struktura
    index := CreateIndex(keys, offset, table.indexFilename)
    keys, offsets := index.Write()
    WriteSummary(keys, offsets, table.summaryFilename)
    Write_BloomFilter(table.filterFilename, filter)
    CreateMerkleTree(values, strings.ReplaceAll(table.SSTableFilename, "data/sstable/", ""))
    table.WriteTOC()

    return
}

func (st *SSTable) SStableFind(key string, offset int64) (nadjen bool, value []byte, timestamp string) {
    nadjen = false 
    timestamp = ""

    file, err := os.Open(st.SSTableFilename)
    if err != nil {
        panic(err)           //otvori fajl
    }
//citanje zaglavlja
    reader := bufio.NewReader(file)   //citac
    bytes := make([]byte, 8)   //niz od 8B
    _, err = reader.Read(bytes)   //cita tih 8B
    if err != nil {
        panic(err)
    }
    fileLen := binary.LittleEndian.Uint64(bytes)   //pretvara ga iz niza bajtova u unsigned int 64
    _, err = file.Seek(offset, 0)  //postavimo poziciju na 0; pozicioniramo se na pocetak fajla
    if err != nil {
        return false, nil, ""   
    }
    reader = bufio.NewReader(file)  //resetuje citac

    var i uint64
    for i = 0; i < fileLen; i++ {
        deleted := false   //u startu postavljamo da slog nije obrisan; ako naidjemo na tombstone=1, menjamo vrednost u true

        // crc
        crcBytes := make([]byte, 4)
        _, err = reader.Read(crcBytes)   //citamo sledeci slog-crc
        if err != nil {
            if err == io.EOF {
                break
            }
            panic(err)
        }
        crcValue := binary.LittleEndian.Uint32(crcBytes)  //pretvara ga iz niza bajtova u unsigned int 32

        // Timestamp
        timestampBytes := make([]byte, 16)
        _, err = reader.Read(timestampBytes)
        if err != nil {
            panic(err)
        }
        timestamp = string(timestampBytes[:])   //pretvori ga u string

        //Tombstone

        tombstone, err := reader.ReadByte()
        if err != nil {
            panic(err)
        }

        if tombstone == 1 {   //obrisan slog
            deleted = true
        }

        // keyLen
        keyLenBytes := make([]byte, 8)
        _, err = reader.Read(keyLenBytes)
        if err != nil {
            panic(err)
        }
        keyLen := binary.LittleEndian.Uint64(keyLenBytes)   //pretvara ga iz niza bajtova u unsigned int 64

        valueLenBytes := make([]byte, 8)
        _, err = reader.Read(valueLenBytes)
        if err != nil {
            panic(err)
        }
        valueLen := binary.LittleEndian.Uint64(valueLenBytes)  //pretvara ga iz niza bajtova u unsigned int 64
//keys
        keyBytes := make([]byte, keyLen)
        _, err = reader.Read(keyBytes)
        if err != nil {
            panic(err)
        }
        nodeKey := string(keyBytes[:])   //pretvara kljuceve u string

        if nodeKey == key {
            nadjen = true    //?
        }
//values
        valueBytes := make([]byte, valueLen)
        _, err = reader.Read(valueBytes)
        if err != nil {
            panic(err)
        }

        if nadjen && !deleted && CRC32(valueBytes) == crcValue {
            value = valueBytes //ako je nadjen, nije obrisan i provera je prosl, vrednosti su te koje je procitao
            break
        } else if nadjen && deleted {   //nadjen i obrisan- vrati false, bez vrednosti, i default vrednost za timestamp
            return false, nil, ""
        }
    }
    file.Close()
    return nadjen, value, timestamp  //ako je sve ok vratiti ove tri vrednosti
}

//fajl koji sadrzi konkretan spisak svih fajlova za konkretan SStable
func (st *SSTable) WriteTOC() {
    filename := st.generalFilename + "TOC.txt"
    file, err := os.Create(filename)   //kreiramo fajl
    if err != nil {
        panic(err)
    }
    defer file.Close()

    writer := bufio.NewWriter(file)  //kreiramo writer

    _, err = writer.WriteString(st.SSTableFilename + "\n")   //upisuje string- naziv SStable fajla i novi vred
    if err != nil {
        return
    }
    _, err = writer.WriteString(st.indexFilename + "\n")   //sledece sto upisuje je naziv index fajla + novi red
    if err != nil {
        return
    }
    _, err = writer.WriteString(st.summaryFilename + "\n")  //sledece upisuje naziv summary fajla i novi red
    if err != nil {
        return
    }
    _, err = writer.WriteString(st.filterFilename)   //naziv filter fajla
    if err != nil {
        return
    }

    err = writer.Flush()
    if err != nil {
        return
    }
}

func readSSTable(filename, level string) (table *SSTable) {
    filename = "data/sstable/usertable-data-ic-" + filename + "-lev" + level + "-TOC.txt"

    file, err := os.Open(filename)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    reader := bufio.NewReader(file)

    //generalFilename, _ := reader.ReadString('\n')
    SSTableFilename, _ := reader.ReadString('\n')
    indexFilename, _ := reader.ReadString('\n')   //citanje naziva fajlova iz strukture sstable
    summaryFilename, _ := reader.ReadString('\n')
    filterFilename, _ := reader.ReadString('\n')
    generalFilename := strings.ReplaceAll(SSTableFilename, "Data.db\n", "")

    table = &SSTable{generalFilename: generalFilename,
        SSTableFilename: SSTableFilename[:len(SSTableFilename)-1], indexFilename: indexFilename[:len(indexFilename)-1],
        summaryFilename: summaryFilename[:len(summaryFilename)-1], filterFilename: filterFilename}

    return
}

//trazi kljuc u BloomFilteru, Summary, Indexu--> vraca key i offset
func (st *SSTable) SSTableQuery(key string) (nadjen bool, value []byte, timestamp string) {
    nadjen = false
    value = nil
    bf := Read_BloomFilter(st.filterFilename)
    nadjen = bf.Query_BF(key)  //da li je kljuc mozda nadjen ili sigurno ne  //ovo trazi kljuc u BloomFilteru
    if nadjen {  //ako je mozda prisutan u bf
        nadjen, offset := FindSummary(key, st.summaryFilename) //onda trazimo u summary
        if nadjen {
            nadjen, offset = FindIndex(key, offset, st.indexFilename)  //ako cuva vrednot u indexu
            if nadjen {
                nadjen, value, timestamp = st.SStableFind(key, offset)   //uzmemo tu vrednost
                if nadjen {
                    return true, value, timestamp
                }
            }
        }
    }
    return false, nil, ""
}

func findSSTableFilename(level string) (filename string) {
    filenameNum := 1   //broj fajla
    filename = strconv.Itoa(filenameNum)  //int u char   //str(1)   //toc fajlovi idu 1,2,...
    possibleFilename := "data/sstable/usertable-data-ic-" + filename + "-lev" + level + "-TOC.txt" //./

    for {
        _, err := os.Stat(possibleFilename)  //ako ga nije nasao, tj. ako fajl ne postoji
        if err == nil {
            filenameNum += 1  //broj fajla se poveca za 1
            filename = strconv.Itoa(filenameNum)  //ta vrednost zapisna kao tip stringa
        } else if errors.Is(err, os.ErrNotExist) {
            return
        }
        possibleFilename = "data/sstable/usertable-data-ic-" + filename + "-lev" + level + "-TOC.txt"  //./

		//ako postoji vraca broj fajla kao str
    }

}

//na osnovu kljuca (i dubine pretrage) vraca najnoviju (naskoriju) vrednost
func SearchThroughSSTables(key string, maxLevels int) (found bool, oldValue []byte) {
    oldTimestamp := ""  
    found = false
    levelNum := maxLevels
	//pocetna granica najveci
	//ide unazad
    for ; levelNum >= 1; levelNum-- {
        level := strconv.Itoa(levelNum)
        maxFilename := findSSTableFilename(level)   //vraca broj kao str
        maxFilenameNum, _ := strconv.Atoi(maxFilename) //char to int
        filenameNum := maxFilenameNum - 1  //3
        for ; filenameNum > 0; filenameNum-- {
            filename := strconv.Itoa(filenameNum)  //3 str
            table := readSSTable(filename, level)  //cita sve fajlove iz toc
            ok, value, timestamp := table.SSTableQuery(key)  //nadjemo vrednost i timestamp kljuca koji smo prosledili
            if oldTimestamp == "" && ok {
                oldTimestamp = timestamp  //16:15  //17:15
                found = true
                oldValue = value  //njenu vrednost  //v2
            } else if oldTimestamp != "" && ok {
                if timestamp > oldTimestamp {  //17:15
                    oldValue = value
                    found = true
                }
            }
        }
    }
    return
}