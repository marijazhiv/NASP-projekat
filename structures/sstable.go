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
    generalFilename string
    SSTableFilename string
    indexFilename   string
    summaryFilename string
    filterFilename  string
}

//kada se MemTable napuni, zapisuje se na disk i formira se SStable
func CreateSStable(data MemTable, filename string) (table *SSTable) {
    //prilikom kreiranja se koristi format imenovanja usertable-GENERATION-ELEMENT.[db/txt]
    generalFilename := "kv-system/data/sstable/usertable-data-ic-" + filename + "-lev1-"
    table = &SSTable{generalFilename, generalFilename + "Data.db", generalFilename + "Index.db",
        generalFilename + "Summary.db", generalFilename + "Filter.gob"}

    filter := CreateBloomFilter(data.Size(), 2)    //bloomfilter za skup kljuceva
    keys := make([]string, 0)
    offset := make([]uint, 0)       //pozicija u sstable-u
    values := make([][]byte, 0)
    currentOffset := uint(0)
    file, err := os.Create(table.SSTableFilename)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    writer := bufio.NewWriter(file)

    // file length
    bytesLen := make([]byte, 8)
    binary.LittleEndian.PutUint64(bytesLen, uint64(data.Size()))   //najmanji bit se cuva na prvoj adresi
    bytesWritten, err := writer.Write(bytesLen)
    currentOffset += uint(bytesWritten)
    if err != nil {
        log.Fatal(err)
    }

    err = writer.Flush()
    if err != nil {
        return
    }

    // We need to check if data is sorted
    for node := data.data.head.Next[0]; node != nil; node = node.Next[0] {
        key := node.Key
        value := node.Value
        keys = append(keys, key)
        offset = append(offset, currentOffset)
        values = append(values, value)

        filter.Add(*node)
        //crc
        crc := CRC32(value)
        crcBytes := make([]byte, 4)
        binary.LittleEndian.PutUint32(crcBytes, crc)
        bytesWritten, err := writer.Write(crcBytes)
        currentOffset += uint(bytesWritten)
        if err != nil {
            return
        }

        //Timestamp
        timestamp := node.Timestamp
        timestampBytes := make([]byte, 19)
        copy(timestampBytes, timestamp)

        bytesWritten, err = writer.Write(timestampBytes)
        if err != nil {
            log.Fatal(err)
        }
        currentOffset += uint(bytesWritten)

        //Tombstone---> ako je podatak obrisan i njegova vrednost
        tombstone := node.Tombstone
        tombstoneInt := uint8(0)
        if tombstone {
            tombstoneInt = 1
        }

        err = writer.WriteByte(tombstoneInt)
        currentOffset += 1
        if err != nil {
            return
        }

        keyBytes := []byte(key)

        keyLen := uint64(len(keyBytes))
        keyLenBytes := make([]byte, 8)
        binary.LittleEndian.PutUint64(keyLenBytes, keyLen)
        bytesWritten, err = writer.Write(keyLenBytes)
        if err != nil {
            log.Fatal(err)
        }
        currentOffset += uint(bytesWritten)

        valueLen := uint64(len(value))
        valueLenBytes := make([]byte, 8)
        binary.LittleEndian.PutUint64(valueLenBytes, valueLen)
        bytesWritten, err = writer.Write(valueLenBytes)
        if err != nil {
            log.Fatal(err)
        }
        currentOffset += uint(bytesWritten)

        bytesWritten, err = writer.Write(keyBytes)
        if err != nil {
            log.Fatal(err)
        }
        currentOffset += uint(bytesWritten)

        bytesWritten, err = writer.Write(value)
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
    writeBloomFilter(table.filterFilename, filter)
    CreateMerkleTree(values, strings.ReplaceAll(table.SSTableFilename, "kv-system/data/sstable/", ""))
    table.WriteTOC()

    return
}

func (st *SSTable) SStableFind(key string, offset int64) (ok bool, value []byte, timestamp string) {
    ok = false
    timestamp = ""

    file, err := os.Open(st.SSTableFilename)
    if err != nil {
        panic(err)
    }

    reader := bufio.NewReader(file)
    bytes := make([]byte, 8)
    _, err = reader.Read(bytes)
    if err != nil {
        panic(err)
    }
    fileLen := binary.LittleEndian.Uint64(bytes)
    _, err = file.Seek(offset, 0)
    if err != nil {
        return false, nil, ""
    }
    reader = bufio.NewReader(file)

    var i uint64
    for i = 0; i < fileLen; i++ {
        deleted := false

        // crc
        crcBytes := make([]byte, 4)
        _, err = reader.Read(crcBytes)
        if err != nil {
            if err == io.EOF {
                break
            }
            panic(err)
        }
        crcValue := binary.LittleEndian.Uint32(crcBytes)

        // Timestamp
        timestampBytes := make([]byte, 19)
        _, err = reader.Read(timestampBytes)
        if err != nil {
            panic(err)
        }
        timestamp = string(timestampBytes[:])

        //Tombstone

        tombstone, err := reader.ReadByte()
        if err != nil {
            panic(err)
        }

        if tombstone == 1 {
            deleted = true
        }

        // keyLen
        keyLenBytes := make([]byte, 8)
        _, err = reader.Read(keyLenBytes)
        if err != nil {
            panic(err)
        }
        keyLen := binary.LittleEndian.Uint64(keyLenBytes)

        valueLenBytes := make([]byte, 8)
        _, err = reader.Read(valueLenBytes)
        if err != nil {
            panic(err)
        }
        valueLen := binary.LittleEndian.Uint64(valueLenBytes)

        keyBytes := make([]byte, keyLen)
        _, err = reader.Read(keyBytes)
        if err != nil {
            panic(err)
        }
        nodeKey := string(keyBytes[:])

        if nodeKey == key {
            ok = true
        }

        valueBytes := make([]byte, valueLen)
        _, err = reader.Read(valueBytes)
        if err != nil {
            panic(err)
        }

        if ok && !deleted && CRC32(valueBytes) == crcValue {
            value = valueBytes
            break
        } else if ok && deleted {
            return false, nil, ""
        }
    }
    file.Close()
    return ok, value, timestamp
}

func (st *SSTable) WriteTOC() {
    filename := st.generalFilename + "TOC.txt"
    file, err := os.Create(filename)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    writer := bufio.NewWriter(file)

    _, err = writer.WriteString(st.SSTableFilename + "\n")
    if err != nil {
        return
    }
    _, err = writer.WriteString(st.indexFilename + "\n")
    if err != nil {
        return
    }
    _, err = writer.WriteString(st.summaryFilename + "\n")
    if err != nil {
        return
    }
    _, err = writer.WriteString(st.filterFilename)
    if err != nil {
        return
    }

    err = writer.Flush()
    if err != nil {
        return
    }
}

func readSSTable(filename, level string) (table *SSTable) {
    filename = "kv-system/data/sstable/usertable-data-ic-" + filename + "-lev" + level + "-TOC.txt"

    file, err := os.Open(filename)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    reader := bufio.NewReader(file)

    //generalFilename, _ := reader.ReadString('\n')
    SSTableFilename, _ := reader.ReadString('\n')
    indexFilename, _ := reader.ReadString('\n')
    summaryFilename, _ := reader.ReadString('\n')
    filterFilename, _ := reader.ReadString('\n')
    generalFilename := strings.ReplaceAll(SSTableFilename, "Data.db\n", "")

    table = &SSTable{generalFilename: generalFilename,
        SSTableFilename: SSTableFilename[:len(SSTableFilename)-1], indexFilename: indexFilename[:len(indexFilename)-1],
        summaryFilename: summaryFilename[:len(summaryFilename)-1], filterFilename: filterFilename}

    return
}

func (st *SSTable) SSTableQuery(key string) (ok bool, value []byte, timestamp string) {
    ok = false
    value = nil
    bf := readBloomFilter(st.filterFilename)
    ok = bf.Query(key)
    if ok {
        ok, offset := FindSummary(key, st.summaryFilename)
        if ok {
            ok, offset = FindIndex(key, offset, st.indexFilename)
            if ok {
                ok, value, timestamp = st.SStableFind(key, offset)
                if ok {
                    return true, value, timestamp
                }
            }
        }
    }
    return false, nil, ""
}

func findSSTableFilename(level string) (filename string) {
    filenameNum := 1
    filename = strconv.Itoa(filenameNum)
    possibleFilename := "./kv-system/data/sstable/usertable-data-ic-" + filename + "-lev" + level + "-TOC.txt"

    for {
        _, err := os.Stat(possibleFilename)
        if err == nil {
            filenameNum += 1
            filename = strconv.Itoa(filenameNum)
        } else if errors.Is(err, os.ErrNotExist) {
            return
        }
        possibleFilename = "./kv-system/data/sstable/usertable-data-ic-" + filename + "-lev" + level + "-TOC.txt"
    }

}

func SearchThroughSSTables(key string, maxLevels int) (found bool, oldValue []byte) {
    oldTimestamp := ""
    found = false
    levelNum := maxLevels
    for ; levelNum >= 1; levelNum-- {
        level := strconv.Itoa(levelNum)
        maxFilename := findSSTableFilename(level)
        maxFilenameNum, _ := strconv.Atoi(maxFilename)
        filenameNum := maxFilenameNum - 1
        for ; filenameNum > 0; filenameNum-- {
            filename := strconv.Itoa(filenameNum)
            table := readSSTable(filename, level)
            ok, value, timestamp := table.SSTableQuery(key)
            if oldTimestamp == "" && ok {
                oldTimestamp = timestamp
                found = true
                oldValue = value
            } else if oldTimestamp != "" && ok {
                if timestamp > oldTimestamp {
                    oldValue = value
                    found = true
                }
            }
        }
    }
    return
}
