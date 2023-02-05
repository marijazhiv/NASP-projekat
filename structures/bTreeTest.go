package structures

import "fmt"

func main() {
	el := BTreeElement{"58", "A", false}
	stablo := CreateBTree(5)
	Insert(stablo, &el)
	el1 := BTreeElement{"36", "B", false}
	Insert(stablo, &el1)
	el2 := BTreeElement{"22", "C", false}
	el3 := BTreeElement{"10", "D", false}
	el4 := BTreeElement{"50", "E", false}
	Insert(stablo, &el2)
	Insert(stablo, &el3)
	Insert(stablo, &el4)
	el5 := BTreeElement{"54", "F", false}
	el6 := BTreeElement{"38", "G", false}
	el7 := BTreeElement{"52", "H", false}
	el9 := BTreeElement{"40", "J", false}
	el10 := BTreeElement{"42", "L", false}
	el11 := BTreeElement{"12", "M", false}
	el12 := BTreeElement{"46", "N", false}
	el13 := BTreeElement{"29", "O", false}
	el14 := BTreeElement{"1", "P", false}
	el15 := BTreeElement{"7", "Q", false}
	el16 := BTreeElement{"5", "R", false}
	el17 := BTreeElement{"39", "S", false}
	Insert(stablo, &el5)
	Insert(stablo, &el6)
	Insert(stablo, &el7)
	Insert(stablo, &el9)
	Insert(stablo, &el10)
	Insert(stablo, &el11)
	Insert(stablo, &el12)
	Insert(stablo, &el13)
	Insert(stablo, &el14)
	Insert(stablo, &el15)
	Insert(stablo, &el16)
	Insert(stablo, &el17)
	Wipe(stablo, &el5)
	Layout(stablo)
	fmt.Println("THE END! :)")
}
