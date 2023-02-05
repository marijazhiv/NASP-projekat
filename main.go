package main

import (
	"NASP-projekat/structures"
	"bufio"
	"fmt"
	"os"
	"strconv"
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

func check_tocken_bucket(s *structures.Structures) bool {
	r := s.TOKEN_BUCKET.ValidateRequest()
	if r == false {
		fmt.Println("Previse zahteva je poslato!")
		return false
	}
	return true
}

func unos() string {
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
	return input.Text()
}

func list_func(choice int, struc *structures.Structures) bool { //ime promenljive nam se kosi sa imenom paketa
	if choice == 1 { //put
		if structures.TOKEN_BUCKET.validateRequest() == true {
			fmt.Print("Unesite kljuc podatka koji zelite da PUT-ujete -> ")
			key := scan()
			fmt.Print("Unesite vrednost podatka koji zelite da PUT-ujete -> ")
			value := scan()
			if structures.PUT(key, []byte(value), false) == true {
				fmt.Println("PUT je uspesno zavrsen!")
			} else {
				fmt.Println("Nije moguce izvrsiti PUT!")
			}
		} else {
			fmt.Println("Previse zahteva je poslato!")
		}
	} else if choice == 2 { //get
		if structures.TOKEN_BUCKET.validateRequest() == true {

		} else {

		}
	}  else if choice == 3 { //delete

	} else if choice == 4 { //list

	} else if choice == 5 { //range scan

	} else if choice == 6 { //create cms

	} else if choice == 7 { //add element to cms

	} else if choice == 8 { //query cms

	} else if choice == 9 { //create hll
		if !check_tocken_bucket(struc) {
			return true
		}

		fmt.Println("\nKreiranje HyperLogLog-a")
		fmt.Print("Kljuc HLL: ")
		key := "hll-" + unos()
		val := structures.NewHLL(uint(struc.CONFIG.HLL_Parameters.Precision))
		hll := val.SerializeHLL()
		if struc.PUT(key, hll, false) {
			fmt.Println("HyperLogLog je uspesno kreiran.")
		}
	} else if choice == 10 { //add element to hll
		if !check_tocken_bucket(struc) {
			return true
		}

		fmt.Println("\n-Dodajemo na HyperLogLog")
		fmt.Print("Kljuc HLL: ")
		key := "hll-" + unos()
		ok, hll := struc.Check_key(key)
		if !ok {
			fmt.Println("Nije nadjen HLL sa zadatim kljucem.")
		}

		hyperll := structures.DeserializeHLL(hll)
		fmt.Print(("Vrednost koju dodajemo: "))
		val := unos()
		hyperll.Add(val)
		if struc.EDIT(key, hyperll.SerializeHLL()) {
			fmt.Println("Uspesno dodat!")
		} else {
			fmt.Println("Neuspesno dodat!")
		}
		return true

	} else if choice == 11 { //calculate hll
		if !check_tocken_bucket(struc) {
			return true
		}

		fmt.Println("\n-Estimacija HLL")
		fmt.Print("Kljuc HLL: ")
		key := "hll-" + unos()
		ok, hll := struc.Check_key(key)
		if !ok {
			fmt.Println("Nije nadjen HLL sa zadatim kljucem.")
		}

		hyperll := structures.DeserializeHLL(hll)
		fmt.Println("Estimacija: ", hyperll.Count())
		return true

	} else if choice == 12 {
		struc.WAL.Dump()
		fmt.Println("Izlazak iz programa!")
		return false
	}

	return true
}

func main() {
	structures := new(structures.Structures)
	structures.Init()

	p := false
	choice := 0
	for p == false { //ponavlja se sve dok input ne bude validan, *broj i u dobrom opsegu*
		list_meni()
		input := bufio.NewScanner(os.Stdin)
		input.Scan()

		choice, err := strconv.Atoi(input.Text())

		if err != nil {
			fmt.Println("Niste uneli broj. Pokusajte ponovo!")
		} else {
			if choice >= 1 && choice <= 12 {
				break
			} else {
				fmt.Println("Dozvoljeni opseg operacija u meniju je 1-12! Pokusajte ponovo!")
			}
		}

	}

	q := true
	for q == true { //sve dok se ne pozove kraj programa, tj 12
		fmt.Println(choice)
		//pozivamo funkciju koja izlistava pozive, od 1-12
		q = list_func(choice, structures)
	}

}
