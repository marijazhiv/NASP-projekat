package structures

import "fmt"

func Test() {
	sl := NewSkipList(32)
	sl.Add("Boris", []byte("Boris"), false)
	sl.Add("Marija", []byte("Marija"), false)
	sl.Add("Igor", []byte("Igor"), false)
	val := sl.Find("Boris")
	fmt.Print((val.Key))
	fmt.Print("\n")
	val2 := sl.Find("Marija")
	fmt.Print((val2.Key))
	sl.Delete(("Marija"))
	fmt.Print("\n")
	val2 = sl.Find("Marija")
	fmt.Print((val2.Key))
}
