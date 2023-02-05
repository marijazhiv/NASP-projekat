package structures

import "time"

// Jelena Adamovic, SV 6/2021

type TokenBucket struct {
	capacity            int   // maksimalan broj tokena koji mozemo smestiti u Token Bucket
	currentTokens       int   // trenutan broj tokena koji se nalaze u Token Bucket-u
	interval            int64 // vreme koje je potrebno da bi se bucket napunio (u sekundama)
	lastRefillTimestamp int64 // vreme poslednjeg punjenja u sekundama
}

// Funkcija NewTokenBucket kreira novi prazan Token Bucket.
// Kapacitet TokenBucket-a preuzimamo iz konfiguracionog fajla.
func NewTokenBucket(interval int64, maximumTokens int) *TokenBucket {
	return &TokenBucket{
		capacity:            maximumTokens,
		currentTokens:       maximumTokens,
		interval:            interval,
		lastRefillTimestamp: time.Now().Unix(),
	}
}

// Provera da li je isteklo vreme nakon kog je potrebno resetovati vremenski brojac.
// Ako vreme nije isteklo, treba proveriti da li korisnik ima dovoljno preostalih zahteva
// da obradi dolazni zahtev.
// Ako korisniku nije preostalo slobodnih zahteva,trenutni zahtev se odbacuje.
// U suprotnom, smanjujemo brojac za 1 i vrsimo obradu dolaznog zahteva.
// Ako je vreme proteklo,tj. razlika resetovanog vremena i trenutnog vremena je veca
// od definisanog intervala, resetujemo broj dozvoljenih zahteva na unapred definisano ogranicenje
// i definisemo novo vreme resetovanja

func (tb *TokenBucket) ValidateRequest() bool {
	if time.Now().Unix()-tb.lastRefillTimestamp > tb.interval {
		tb.lastRefillTimestamp = time.Now().Unix()
		tb.currentTokens = tb.capacity
	}
	if tb.currentTokens <= 0 {
		return false
	}
	tb.currentTokens--
	return true
}

// promenaaa
