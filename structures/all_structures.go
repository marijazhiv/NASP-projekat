package structures

import (
	"NASP-projekat/configurations"
	"fmt"
	"time"
)

type Structures struct {
	WAL          *Wal
	CACHE        *Cache
	LSM          *LSM
	TOKEN_BUCKET *TokenBucket
	MEM_TABLE    *MemTable
	CONFIG       *configurations.Config
}

func (s *Structures) Init() { //kreiramo prazne strukture na pocetku programa
	s.CONFIG = configurations.Get_Configurations()
	s.WAL = CreateWal(WAL_path)
	s.CACHE = Create_Cache(s.CONFIG.Cache_Parameters.Max_Data)
	s.LSM = create_LSM(s.CONFIG.LSM_Parameters.Max_Level, s.CONFIG.LSM_Parameters.Level_Size)
	s.MEM_TABLE = NewMemTable(uint(s.CONFIG.MemTable_Parameters.Max_Size),
		uint(s.CONFIG.MemTable_Parameters.Limit), s.CONFIG.MemTable_Parameters.Skip_List_Max_Height)

	interval := int64(s.CONFIG.TokenBucket_Parameters.Interval)
	s.TOKEN_BUCKET = NewTokenBucket(interval, s.CONFIG.TokenBucket_Parameters.Max_Tokens)
}

func (s *Structures) PUT(key string, value []byte, tombstone bool) bool { //dodavanje elementa koji sadrzi kljuc i vrednost, uz vodjenje racuna o svim strukturama
	element := Element{
		Key:       key,
		Value:     value,
		Next:      nil,
		Timestamp: time.Now().String(),
		Tombstone: tombstone,
		Checksum:  CRC32(value),
	}

	s.WAL.Put(&element)
	s.MEM_TABLE.AddNew(key, value, tombstone)
	s.CACHE.Add_Node(key, value)

	if s.MEM_TABLE.CheckFlush() == true {
		s.MEM_TABLE.Flush()
		s.WAL.RemoveSegments()
		s.LSM.DoWeNeedCompaction("data/sstable/", 1)
		s.MEM_TABLE = NewMemTable(uint(s.CONFIG.MemTable_Parameters.Max_Size),
			uint(s.CONFIG.MemTable_Parameters.Limit), s.CONFIG.MemTable_Parameters.Skip_List_Max_Height)
	}

	return true
}

func (s *Structures) Check_key(key string) (bool, []byte) { //dobaljanje elementa uz pomocu njegovog kljuca, gde je povratna vrednost podatak da li je element pronadjen i vrednost elementa
	p, delete, value := s.MEM_TABLE.Find(key)
	if p == true && delete == true { //element je pronadjen ali ima status obrisanog elementa
		return false, nil
	} else if p == true { //element je pronadjen
		s.CACHE.Add_Node(key, value)
		return true, value
	}

	//element nije pronadjen u memoriji, proveravamo cache
	p, value = s.CACHE.Get_Node(key) //ako je p = true, pronadjen je
	if p == true {
		s.CACHE.Add_Node(key, value)
		return true, value
	}

	//element nije pronadjen u cache-u
	p, value = SearchThroughSSTables(key, s.CONFIG.LSM_Parameters.Max_Level) //vraca p = true ako je pronadjen element, i najskoriju vrednost prosledjenog kljuca
	if p == true {
		s.CACHE.Add_Node(key, value)
		return true, value
	}

	return false, nil //nigde nije pronadjen element sa kljucem
}

func (s *Structures) GET(key string) string {
	var text string
	p, value := s.Check_key(key) //proveravamo da li se element nalazi u memoriji, cache-u ili sstable-u

	if p == false { //ako se element ne nalazi u memoriji, cache-u ili sstable-u
		p, value = s.Check_key("hll-" + key)
		if p == true {
			hll := DeserializeHLL(value)
			text = "Podatak pripada Hyper_Log_Log-u sa estimacijom: " + fmt.Sprintf("%d", hll.Count())
		} else {
			p, value = s.Check_key("csm-" + key)
			if p == true {
				text = "Podatak pripada Count_Min_Skatch-u"
			} else {
				text = "Podatak sa kljucem koji je unet ne postoji!"
			}
		}
	} else {
		text = string(value)
	}

	return text
}

func (s *Structures) DELETE(key string) bool {

	if s.MEM_TABLE.Remove(key) {
		s.CACHE.Delete_Node(key)
		return true
	}
	if s.MEM_TABLE.Remove("hll-" + key) {
		s.CACHE.Delete_Node("hll-" + key)
		return true
	}
	if s.MEM_TABLE.Remove("cms-" + key) {
		s.CACHE.Delete_Node("cms-" + key)
		return true
	}

	//ako elemnt nije u mem_table strukturi
	p, value := s.Check_key(key)
	if p == false {
		key_hll := "hll-" + key
		p, value = s.Check_key(key_hll)
		if p == false {
			key_cms := "cms-" + key
			p, value = s.Check_key(key_cms)
			if p == false {
				return false
			} else {
				key = key_cms
			}
		} else {
			key = key_hll
		}
	}

	s.PUT(key, value, true) //dodajemo podatak o tome da je element obrisan
	s.CACHE.Delete_Node(key)
	return true
}

func (s *Structures) EDIT(key string, value []byte) bool {
	s.MEM_TABLE.Edit(key, value, false)
	elem := Element{
		Key:       key,
		Value:     value,
		Next:      nil,
		Timestamp: time.Now().String(),
		Tombstone: false,
		Checksum:  CRC32(value),
	}
	s.WAL.Put(&elem)
	s.CACHE.Add_Node(key, value)
	return true
}

// u zahtev uklju£iti i veli£inu stranice i redni broj stranice koju ºelite da
// vam sistem vrati
func (s *Structures) LIST() {

}

func (s *Structures) RANGE_SCAN() {

}
