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
func (mt *MemTable) AddNew(key string, value []byte, tombstone bool) {
	mt.size += 1
	mt.data.Add(key, value, tombstone)
}

// Funkcija Remove vrsi uklanjanje elementa iz MemTable i radi proveru da li je element obrisan.
// Ako jeste, vraca true, ako nije - vraca false.
func (mt *MemTable) Remove(key string) bool {
	deleted := mt.data.Delete(key)
	if deleted == nil {
		return false
	}
	return true
}

// Funkcija za izmenu elementa MemTable-a.
// Funkcija zapocinje trazenjem kljuca sa odgovarajucim elementom u MemTable.
// Ako element ne postoji, pravimo novi.
// Ako postoji, radimo izmenu.
func (mt *MemTable) Edit(key string, value []byte, tombstone bool) {
	node := mt.data.Find(key)
	if node == nil {
		mt.data.Add(key, value, tombstone)
	} else {
		node.Value = value
	}

}

// Funkcija za pretragu elemenata u MemTable po kljucu.
// Moze nam dati informaciju o tome da li je element pronadjen,
// da li je obrisan i moze vratiti njegovu vrednost tipa string.
func (mt *MemTable) Find(key string) (ok, flag bool, value []byte) {
	node := mt.data.Find(key)
	if node == nil {
		ok = false
		flag = false
		value = nil
	} else if node.Tombstone {
		ok = true
		flag = true
		value = nil
	} else {
		ok = true
		flag = false
		value = node.Value
	}
	return
}

// Funkcija Size vraca velicinu MemTabele.
func (mt *MemTable) Size() uint {
	return mt.size
}

// Funkcija proverava da li je MemTable dovoljno popunjen da bi se flush-ovao u SSTable.
func (mt *MemTable) CheckFlush() bool {
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
