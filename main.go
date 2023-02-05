package main

import (
	"bufio"
	"fmt"
	"naisp_projekat/structures"
	"os"
	"strconv"
	"strings"
)

func list_meni() {
	fmt.Println("Meni: ")
	fmt.Println("1. PUT")
	fmt.Println("2. GET")
	fmt.Println("3. DELETE")
	fmt.Println("4. LIST")
	fmt.Println("5. RANGE SCAN")
	fmt.Println("More Structures: ")
	fmt.Println("6. Create Count_Min_Sketch")
	fmt.Println("7. Add Element in Count_Min_Sketch")
	fmt.Println("8. Query Count_Min_Sketch")
	fmt.Println("9. Create Hyper_Log_Log")
	fmt.Println("10. Add Element in Hyper_Log_Log")
	fmt.Println("11. Calculate Hyper_Log_Log")
	fmt.Println("12. Izlaz iz programa")

	fmt.Println("Izaberite jednu od ponudjenih opcija iz menija -> ")
}

func list_func(choice int, s *structures.Structures) bool {
	if choice == 1 { //put
		if s.TOKEN_BUCKET.ValidateRequest() == true {
			fmt.Print("Unesite kljuc podatka koji zelite da PUT-ujete -> ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := input_key.Text()
			fmt.Print("Unesite vrednost podatka koji zelite da PUT-ujete -> ")
			input_value := bufio.NewScanner(os.Stdin)
			input_value.Scan()
			value := input_value.Text()
			if s.PUT(key, []byte(value), false) == true {
				fmt.Println("PUT je uspesno zavrsen!")
			} else {
				fmt.Println("Nije moguce izvrsiti PUT!")
			}
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 2 { //get
		if s.TOKEN_BUCKET.ValidateRequest() == true {
			fmt.Print("Unesite kljuc podatka koji pretrazujete -> ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := input_key.Text()

			value := s.GET(key)
			fmt.Println("Vrednost -> ", value)
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 3 { //delete
		if s.TOKEN_BUCKET.ValidateRequest() == true {
			fmt.Print("Unesite kljuc podatka koji zelite da obrisete -> ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := input_key.Text()

			if s.DELETE(key) == true {
				fmt.Println("Podataka je uspesno obrisan!")
			} else {
				fmt.Println("Nije moguce obrisati podatak sa zadatim kljucem. Ne postoji!")
			}
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 4 { //list

	} else if choice == 5 { //range scan

	} else if choice == 6 { //create cms
		if s.TOKEN_BUCKET.ValidateRequest() == true {
			fmt.Print("Unesite kljuc podatka koji zelite da dodate i zatim kreirate CMS -> ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := "cms-" + input_key.Text()

			value := structures.Create_CountMinSketch(0.1, 0.01).Serialize_CMS()
			if s.PUT(key, value, false) == true {
				fmt.Println("Count_Min_Sketch je uspesno kreiran!")
			}
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 7 { //add element to cms
		if s.TOKEN_BUCKET.ValidateRequest() == true {
			fmt.Print("Unesite kljuc podatka koji zelite da dodate u CMS -> ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := "cms-" + input_key.Text()
			p, data := s.Check_key(key)
			if p == false {
				fmt.Println("CMS sa kljucem koji ste uneli ne postoji!")
				return true
			}
			cms := structures.Deserialize_CMS(data)
			fmt.Print("Unesite vrednost podatka koji zelite da dodate u CMS -> ")
			input_value := bufio.NewScanner(os.Stdin)
			input_value.Scan()
			value := input_value.Text()

			cms.Add_Element_CMS(strings.ToUpper(value))
			if s.EDIT(key, cms.Serialize_CMS()) {
				fmt.Println("Uspesno dodata vrednost u CMS!")
			} else {
				fmt.Println("Trenutno nije moguce uneti vrednost u CMS!")
			}
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 8 { //query cms
		if s.TOKEN_BUCKET.ValidateRequest() == true {
			fmt.Print("Unesite kljuc iz CMS -> ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := "cms-" + input_key.Text()
			p, data := s.Check_key(key)
			if p == false {
				fmt.Println("Nije moguce pronaci podatak sa zadatim kljucem!")
				return true
			}
			fmt.Print("Unesite vrednost za koju radite QUERY -> ")
			input_value := bufio.NewScanner(os.Stdin)
			input_value.Scan()
			value := input_value.Text()
			cms := structures.Deserialize_CMS(data)
			fmt.Println(value, " -> ", cms.Query_CMS(strings.ToUpper(value)))
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 9 { //create hll
		if s.TOKEN_BUCKET.ValidateRequest() == false {
			fmt.Println("\nKreiranje HyperLogLog-a")
			fmt.Print("Kljuc HLL: ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := "hll-" + input_key.Text()
			val := structures.NewHLL(uint(s.CONFIG.HLL_Parameters.Precision))
			hll := val.SerializeHLL()
			if s.PUT(key, hll, false) {
				fmt.Println("HyperLogLog je uspesno kreiran.")
			}
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 10 { //add element to hll
		if s.TOKEN_BUCKET.ValidateRequest() == false {
			fmt.Println("\n-Dodajemo na HyperLogLog")
			fmt.Print("Kljuc HLL: ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := "hll-" + input_key.Text()
			ok, hll := s.Check_key(key)
			if !ok {
				fmt.Println("Nije nadjen HLL sa zadatim kljucem.")
			}

			hyperll := structures.DeserializeHLL(hll)
			fmt.Print(("Vrednost koju dodajemo: "))
			input_value := bufio.NewScanner(os.Stdin)
			input_value.Scan()
			val := input_value.Text()
			hyperll.Add(val)
			if s.EDIT(key, hyperll.SerializeHLL()) {
				fmt.Println("Uspesno dodat!")
			} else {
				fmt.Println("Neuspesno dodat!")
			}
			return true
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}

	} else if choice == 11 { //calculate hll
		if s.TOKEN_BUCKET.ValidateRequest() == false {
			fmt.Println("\n-Estimacija HLL")
			fmt.Print("Kljuc HLL: ")
			input_key := bufio.NewScanner(os.Stdin)
			input_key.Scan()
			key := "hll-" + input_key.Text()
			ok, hll := s.Check_key(key)
			if !ok {
				fmt.Println("Nije nadjen HLL sa zadatim kljucem.")
			}

			hyperll := structures.DeserializeHLL(hll)
			fmt.Println("Estimacija: ", hyperll.Count())
			return true
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 12 {
		s.WAL.Dump()
		fmt.Println("Izlazak iz programa!")
		return false
	}

	return true
}

func main() {
	s := new(structures.Structures)
	s.Init()

	p := false
	q := true
	for q == true {
		for p == false { //ponavlja se sve dok input ne bude validan, *broj i u dobrom opsegu*
			list_meni()
			input := bufio.NewScanner(os.Stdin)
			input.Scan()

			choice, err := strconv.Atoi(input.Text())

			if err != nil {
				fmt.Println("Niste uneli broj. Pokusajte ponovo!")
			} else {
				if choice >= 1 && choice <= 12 {
					q = list_func(choice, s)
					break
				} else {
					fmt.Println("Dozvoljeni opseg operacija u meniju je 1-12! Pokusajte ponovo!")
				}
			}

		}

		//sve dok se ne pozove kraj programa, tj 12
		//pozivamo funkciju koja izlistava pozive, od 1-12
	}

}
