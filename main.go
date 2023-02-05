package main

import (
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

func list_func(choice int) bool {
	if choice == 1 { //put

	} else if choice == 2 { //get

	} else if choice == 3 { //delete

	} else if choice == 4 { //list

	} else if choice == 5 { //range scan

	} else if choice == 6 { //create cms

	} else if choice == 7 { //add element to cms

	} else if choice == 8 { //query cms

	} else if choice == 9 { //create hll

	} else if choice == 10 { //add element to hll

	} else if choice == 11 { //calculate hll

	} else if choice == 12 {
		fmt.Println("Izlazak iz programa!")
		return false
	}

	return true
}

func main() {
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
		q = list_func(choice)
	}

}
