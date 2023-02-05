package structures

import "fmt"

func Test() {
	sl := NewSkipList(32)
	sl.add("Boris")
	sl.add("Marija")
	sl.add("Igor")
	_, val := sl.find("Boris")
	fmt.Print((val))
	fmt.Print("\n")
	_, val2 := sl.find("Marija")
	fmt.Print((val2))
	sl.delete(("Marija"))
	fmt.Print("\n")
	_, val2 = sl.find("Marija")
	fmt.Print((val2))
}
