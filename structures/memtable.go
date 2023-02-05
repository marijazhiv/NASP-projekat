package structures

// Jelena Adamovic, SV 6/2021

type MemTable struct {
	data    SkipLista
	size    uint
	limit   uint
	maxSize uint
}

// TODO: preuzimanje maksimalne velicine MemTable iz konfiguracionog fajla
// Funkcija newMemTable pravi novi MemTable uz pomoc skip liste.
// Border predstavlja granicu do koje mozemo napuniti MemTable pre ubacivanja u SSTable.
func NewMemTable(maxSize, limit uint, maxh int) *MemTable {
	sl := NewSkipList(maxh) // pravimo praznu skip listu
	mt := MemTable{*sl, 0, limit, maxSize}
	return &mt
}

// Funkcija AddNew dodaje novi element u MemTable.
// Kljuc tog novog elementa je string key.
func (mt *MemTable) AddNew(key string) {
	mt.size += 1
	mt.data.add(key)
}

// Funkcija Remove vrsi uklanjanje elementa iz MemTable i radi proveru da li je element obrisan.
// Ako jeste, vraca true, ako nije - vraca false.
func (mt *MemTable) Remove(key string) bool {
	deleted := mt.data.delete(key)
	if deleted == true {
		return true
	}
	return false
}

// Funkcija za izmenu elementa MemTable-a.
// Funkcija zapocinje trazenjem kljuca sa odgovarajucim elementom u MemTable.
// Ako element ne postoji, pravimo novi.
// Ako postoji, radimo izmenu.
func (mt *MemTable) Edit(key string, item string) {
	_, flag := mt.data.find(key)

	if flag == false {
		mt.data.add(key)
	} else {
		mt.data.delete(key)
		mt.data.add(item)
	}

}

// Funkcija za pretragu elemenata u MemTable po kljucu.
// Moze nam dati informaciju o tome da li je element pronadjen,
// da li je obrisan i moze vratiti njegovu vrednost tipa string.
func (mt *MemTable) Find(key string) (ok, deleted bool, value string) {
	node, flag := mt.data.find(key)
	if flag == false {
		ok = false
		deleted = false
		value = ""
	} else if node.tombstone {
		ok = true
		deleted = true
		value = ""
	} else {
		ok = true
		deleted = false
		value = node.item
	}
	return
}

// Funkcija Size vraca velicinu MemTabele.
func (mt *MemTable) Size() uint {
	return mt.size
}

// Funkcija proverava da li je MemTable dovoljno popunjen da bi se flush-ovao u SSTable.
func (mt *MemTable) checkFlush() bool {
	if (float64(mt.size)/float64(mt.maxSize))*100 >= float64(mt.limit) {
		return true
	}
	return false
}

// funkcija za flush-ovanje u SSTable
func (mt *MemTable) Flush() {
	filename := findSSTableFilename("1")
	CreateSStable(*mt, filename)
}
