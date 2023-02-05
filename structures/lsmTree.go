package structures

// Jelena Adamovic, SV 6/2021
import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type LSM struct {
	maxLvl  int // maksimalan broj nivoa LSM stabla
	maxSize int // maksimalna velicina LSM stabla
}

// Funkcija create_LSM pravi novo prazno LSM stablo.
func create_LSM(maxL, maxS int) *LSM {
	return &LSM{
		maxLvl:  maxL, // maksimalan broj nivoa LSM stabla
		maxSize: maxS, // maksimalna velicina LSM stabla
	}
}

// Spajanje fajlova
func mergeFiles(dir, fD_File, fI_File, fS_File, fT_File, fF_File, sD_File, sI_File, sS_File,
	sT_File, sF_File string, level, numFile int) {

	str_Level := strconv.Itoa(level + 1) // pretvaramo noivo iz int-a u string

	// general_Filename predstavlja generalni naziv fajla
	// ima delove koji se menjaju u zavisnosti od prosledjenih vrednosti
	general_Filename := dir + "userTable-data-ic-" + strconv.Itoa(numFile) + "-lvl" + str_Level + "-"

	// pravimo novi SSTable
	table := &SSTable{general_Filename, general_Filename + "Data.db",
		general_Filename + "Index.db", general_Filename + "Summary.db",
		general_Filename + "Filter.gob"}

	// Kreiramo novi .db fajl
	new_Data, _ := os.Create(general_Filename + "Data.db")

	current_offset := uint(0)  // trenutni offset u novom fajlu
	current_offset1 := uint(0) // trenutni offset u prvom fajlu
	current_offset2 := uint(0) // trenutni offset u drugom fajlu

	// writer za upis novih podataka u fajl
	writer := bufio.NewWriter(new_Data)

	// duzina fajla - na pocetku ce biti 0
	bytes_Len := make([]byte, 8) // u promenljivu bytes_Len se smesta niz bajtova
	bytes_written, err := writer.Write(bytes_Len)
	current_offset += uint(bytes_written)
	if err != nil {
		log.Fatal(err)
	}

	// otvaranje fajla f_DataFile - prvog fajla
	f_DataFile, err := os.Open(dir + fD_File)
	if err != nil {
		panic(err)
	}

	// otvaranje s_DataFile - drugog fajla
	s_DataFile, err := os.Open(dir + fS_File)
	if err != nil {
		panic(err)
	}

	// citanje podataka iz fajla
	// duzina prvog fajla
	reader1 := bufio.NewReader(f_DataFile)
	bytes := make([]byte, 8)
	_, err = reader1.Read(bytes)
	if err != nil {
		panic(err)
	}

	// serijalizacija podataka u bajtove
	fileLength1 := binary.LittleEndian.Uint64(bytes)
	current_offset1 += 8

	// citanje podataka iz drugog fajla
	// duzina drugog fajla
	reader2 := bufio.NewReader(s_DataFile)
	bytes = make([]byte, 8)
	_, err = reader2.Read(bytes)
	if err != nil {
		panic(err)
	}

	fileLength2 := binary.LittleEndian.Uint64(bytes)
	current_offset2 += 8

	fileLength := Read_And_Write(current_offset, current_offset1, current_offset2, new_Data,
		f_DataFile, s_DataFile, fileLength1, fileLength2, table, level)

	// upis duzine fajla
	Size_Of_File(general_Filename+"Data.db", fileLength)

	_ = new_Data.Close()

	_ = f_DataFile.Close()
	_ = s_DataFile.Close()
	_ = os.Remove(dir + fD_File)
	_ = os.Remove(dir + fI_File)
	_ = os.Remove(dir + fS_File)
	_ = os.Remove(dir + fT_File)
	_ = os.Remove(dir + fF_File)
	_ = os.Remove(dir + sD_File)
	_ = os.Remove(dir + sI_File)
	_ = os.Remove(dir + sS_File)
	_ = os.Remove(dir + sT_File)
	_ = os.Remove(dir + sF_File)

}

// Citanje i upis
func Read_And_Write(current_offset, current_offset1, current_offset2 uint, new_Data, fData_File, sData_File *os.File, file_Length1, file_Length2 uint64, table *SSTable, level int) uint64 {

	filter := Create_BloomFilter(uint(file_Length1+file_Length2), 2)

	keys := make([]string, 0)
	offset := make([]uint, 0)
	values := make([][]byte, 0) // da li ovde treba string

	crc1, timeStamp1, tombStone1, keyLength1, valueLength1, key1, value1, current_offset1 :=
		Read_Data(fData_File, current_offset1)

	crc2, timeStamp2, tombStone2, keyLength2, valueLength2, key2, value2, current_offset2 :=
		Read_Data(sData_File, current_offset2)

	first := uint64(0)
	second := uint64(0)

	for {
		if file_Length1 == first || file_Length2 == second {
			break
		}
		if key1 == key2 {
			if timeStamp1 > timeStamp2 {
				if tombStone1 == 0 {
					offset = append(offset, current_offset)
					current_offset = Write_Data(new_Data, current_offset, crc1, timeStamp1,
						tombStone1, keyLength1, valueLength1, key1, value1)
					filter.Add_Element_BF(Element{key1, nil, nil, timeStamp1, false, 0})
					keys = append(keys, key1)
					values = append(values, []byte(value1))

				}
			} else {
				if tombStone2 == 0 {
					offset = append(offset, current_offset)
					current_offset = Write_Data(new_Data, current_offset, crc2, timeStamp2, tombStone2,
						keyLength2, valueLength2, key2, value2)
					filter.Add_Element_BF(Element{key2, nil, nil, timeStamp2, false, 0})
					keys = append(keys, key2)
					values = append(values, []byte(value2))

				}
			}
			if file_Length1-1 > first {
				crc1, timeStamp1, tombStone1, keyLength1, valueLength1,
					key1, value1, current_offset1 = Read_Data(fData_File, current_offset1)

			}
			first++

			if file_Length2-1 > second {
				crc2, timeStamp2, tombStone2, keyLength2, valueLength2,
					key2, value2, current_offset2 = Read_Data(sData_File, current_offset2)
			}
			second++
		} else if key1 < key2 {
			if tombStone1 == 0 {
				offset = append(offset, current_offset)
				current_offset = Write_Data(new_Data, current_offset, crc1, timeStamp1, tombStone1,
					keyLength1, valueLength1, key1, value1)
				filter.Add_Element_BF(Element{key1, nil, nil, timeStamp1, false, 0})
				keys = append(keys, key1)
				values = append(values, []byte(value1))

			}
			if file_Length1-1 > first {
				crc1, timeStamp1, tombStone1, keyLength1, valueLength1,
					key1, value1, current_offset1 = Read_Data(fData_File, current_offset1)
			}
			first++
		} else {
			if tombStone2 == 0 {
				offset = append(offset, current_offset)
				current_offset = Write_Data(new_Data, current_offset, crc2, timeStamp2,
					tombStone2, keyLength2, valueLength2, key2, value2)
				filter.Add_Element_BF(Element{key2, nil, nil, timeStamp2, false, 0})
				keys = append(keys, key2)
				values = append(values, []byte(value2))

			}
			if file_Length2-1 > second {
				crc2, timeStamp2, tombStone2, keyLength2, valueLength2,
					key2, value2, current_offset2 = Read_Data(sData_File, current_offset2)
			}
			second++
		}
	}
	if file_Length1 == first && file_Length2 != second {
		if file_Length2 != second {
			if tombStone2 == 0 {
				offset = append(offset, current_offset)
				current_offset = Write_Data(new_Data, current_offset, crc2, timeStamp2, tombStone2,
					keyLength2, valueLength2, key2, value2)
				filter.Add_Element_BF(Element{key2, nil, nil, timeStamp2, false, 0})
				keys = append(keys, key2)
				values = append(values, []byte(value2))
			}
			if file_Length2-1 > second {
				crc2, timeStamp2, tombStone2, keyLength2, valueLength2,
					key2, value2, current_offset2 = Read_Data(sData_File, current_offset2)
			}
			second++
		}
	} else if file_Length2 == second && file_Length1 != first {
		if file_Length1 > first {
			if tombStone1 == 0 {
				offset = append(offset, current_offset)
				current_offset = Write_Data(new_Data, current_offset, crc1, timeStamp1,
					tombStone1, keyLength1, valueLength1, key1, value1)
				filter.Add_Element_BF(Element{key1, nil, nil, timeStamp1, false, 0})
				keys = append(keys, key1)
				values = append(values, []byte(value2))
			}
			if file_Length1-1 != first {
				crc1, timeStamp1, tombStone1, keyLength1, valueLength1,
					key1, value1, current_offset1 = Read_Data(fData_File, current_offset1)
			}
			first++
		}
	}
	index := CreateIndex(keys, offset, table.indexFilename)
	keysIndex, offsets := index.Write()
	WriteSummary(keysIndex, offsets, table.summaryFilename)
	table.WriteTOC()
	Write_BloomFilter(table.filterFilename, filter)
	Create_Merkle(level, new_Data.Name(), values)

	return uint64(len(keys))
}

// Upis podataka
func Write_Data(file *os.File, current_offset uint, crc_bytes []byte, timestamp string,
	tombstone byte, keyLength, valueLength uint64, key, value string) uint {

	if tombstone == 1 {
		return current_offset
	}

	file.Seek(int64(current_offset), 0)
	writer := bufio.NewWriter(file)

	bytes_written, err := writer.Write(crc_bytes)
	current_offset += uint(bytes_written)
	if err != nil {
		log.Fatal(err)
	}

	timestamp_bytes := make([]byte, 19)
	copy(timestamp_bytes, timestamp)
	bytes_written, err = writer.Write(timestamp_bytes)
	if err != nil {
		log.Fatal(err)
	}

	tombstone_int := tombstone
	err = writer.WriteByte(tombstone_int)
	current_offset += 1
	if err != nil {
		log.Fatal(err)
	}

	keyLength_Bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLength_Bytes, keyLength)
	bytes_written, err = writer.Write(keyLength_Bytes)
	if err != nil {
		log.Fatal(err)
	}
	current_offset += uint(bytes_written)

	valueLength_Bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLength_Bytes, valueLength)
	bytes_written, err = writer.Write(valueLength_Bytes)
	if err != nil {
		log.Fatal(err)
	}
	current_offset += uint(bytes_written)

	key_Bytes := []byte(key)
	bytes_written, err = writer.Write(key_Bytes)
	if err != nil {
		log.Fatal(err)
	}
	current_offset += uint(bytes_written)

	value_Bytes := []byte(value)
	bytes_written, err = writer.Write(value_Bytes)
	if err != nil {
		log.Fatal(err)
	}
	current_offset += uint(bytes_written)

	err = writer.Flush()
	if err != nil {
		log.Fatal(err)
	}
	return current_offset
}

// Citanje podataka iz fajla
func Read_Data(file *os.File, current_offset uint) ([]byte, string, byte,
	uint64, uint64, string, string, uint) {

	file.Seek(int64(current_offset), 0)
	reader := bufio.NewReader(file)

	crc_bytes := make([]byte, 4)
	_, err := reader.Read(crc_bytes)
	if err != nil {
		panic(err)
	}

	current_offset += 4

	timeStamp_bytes := make([]byte, 19)
	_, err = reader.Read(timeStamp_bytes)
	if err != nil {
		panic(err)
	}

	timeStamp := string(timeStamp_bytes[:])
	current_offset += 19

	tombstone, err := reader.ReadByte()
	if err != nil {
		panic(err)
	}

	current_offset += 1

	keyLengthBytes := make([]byte, 8)
	_, err = reader.Read(keyLengthBytes)
	if err != nil {
		panic(err)
	}

	keyLength := binary.LittleEndian.Uint64(keyLengthBytes)
	current_offset += 8

	valueLengthBytes := make([]byte, 8)
	_, err = reader.Read(valueLengthBytes)
	if err != nil {
		panic(err)
	}

	valueLength := binary.LittleEndian.Uint64(valueLengthBytes)
	current_offset += 8

	key_Bytes := make([]byte, keyLength)
	_, err = reader.Read(key_Bytes)
	if err != nil {
		panic(err)
	}

	key := string(key_Bytes[:])
	current_offset += uint(keyLength)

	value_Bytes := make([]byte, valueLength)
	_, err = reader.Read(value_Bytes)
	if err != nil {
		panic(err)
	}

	value := string(value_Bytes[:])
	current_offset += uint(valueLength)

	return crc_bytes, timeStamp, tombstone, keyLength, valueLength, key, value, current_offset
}

// Provera velicine fajla
func Size_Of_File(filename string, length uint64) {
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	_, err = file.Seek(0, 0)

	writer := bufio.NewWriter(file)

	bytes_length := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes_length, length)
	_, err = writer.Write(bytes_length)

	if err != nil {
		log.Println(err)
	}

	err = writer.Flush()
	if err != nil {
		return
	}

	err = file.Close()
}

// Funkcija Find_Files pronalazi fajlove i vraca ih kao liste stringova
func Find_Files(dir string, level int) ([]string, []string, []string, []string, []string) {
	substr := strconv.Itoa(level)

	files, _ := os.ReadDir(dir)

	var data_Files []string
	var index_Files []string
	var summary_Files []string
	var TOC_Files []string
	var filter_Files []string

	for _, f := range files {
		if strings.Contains(f.Name(), "lvl"+substr+"-data.db") {
			data_Files = append(data_Files, f.Name())
		}

		if strings.Contains(f.Name(), "lvl"+substr+"-index.db") {
			index_Files = append(index_Files, f.Name())
		}

		if strings.Contains(f.Name(), "lvl"+substr+"-summary.db") {
			summary_Files = append(summary_Files, f.Name())
		}

		if strings.Contains(f.Name(), "lvl"+substr+"-TOC.txt") {
			TOC_Files = append(TOC_Files, f.Name())
		}

		if strings.Contains(f.Name(), "lvl"+substr+"-data.db") {
			filter_Files = append(filter_Files, f.Name())
		}
	}
	return data_Files, index_Files, summary_Files, TOC_Files, filter_Files
}

func Create_Merkle(level int, newData string, values [][]byte) {
	files, _ := os.ReadDir("./data/metadata")
	for _, f := range files {
		if strings.Contains(f.Name(), "lvl"+strconv.Itoa(level)+"-metadata.txt") {
			err := os.Remove("./data/metadata" + f.Name())
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	filename := strings.ReplaceAll(newData, "./data/sstable", "")
	CreateMerkleTree(values, filename)
}

// Provera da li smo dosli do granice gde treba izvrsiti kompakciju.
func (lsm LSM) DoWeNeedCompaction(dir string, level int) (bool, []string, []string, []string, []string, []string) {
	data_Files, index_Files, summary_Files, TOC_Files, filter_Files := Find_Files(dir, level)
	return len(index_Files) == lsm.maxSize, data_Files, index_Files, summary_Files, TOC_Files, filter_Files
}

// Funkcija MakeACompaction vrsi kompakciju
func (lsm LSM) MakeACompaction(dir string, level int) {
	if level >= lsm.maxLvl {
		return
	}

	compaction, data_Files, index_Files, summary_Files, TOC_Files, filter_Files := lsm.DoWeNeedCompaction(dir, level)
	if !compaction {
		return
	}

	_, indexFiles_LvlUp, _, _, _ := Find_Files(dir, level+1)

	i := 0
	var numFile int

	if len(indexFiles_LvlUp) == 0 {
		numFile = 1
	} else {
		numFile = len(indexFiles_LvlUp) + 1
	}

	for i < lsm.maxSize {
		f_DataFile := data_Files[i]
		f_IndexFile := index_Files[i]
		f_SummaryFile := summary_Files[i]
		f_TOCFile := TOC_Files[i]
		f_FilterFile := filter_Files[i]
		s_DataFile := data_Files[i+1]
		s_IndexFile := index_Files[i+1]
		s_SummaryFile := summary_Files[i+1]
		s_TOCFile := TOC_Files[i+1]
		s_FilterFile := filter_Files[i+1]
		mergeFiles(dir, f_DataFile, f_IndexFile, f_SummaryFile, f_TOCFile, f_FilterFile,
			s_DataFile, s_IndexFile, s_SummaryFile, s_TOCFile, s_FilterFile, numFile, level)
		i = i + 2
		numFile++
	}

	lsm.MakeACompaction(dir, level+1)

}
