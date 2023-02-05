package structures

import "fmt"

var global = 0

type BTreeElement struct {
	key  string
	item string
	flag bool
}
type BTreeNode struct {
	m           int64
	currentSize int64
	array       []*BTreeElement
	children    []*BTreeNode
	parent      *BTreeNode
}

func Layout(tree *BTreeNode) []*BTreeElement {
	array := make([]*BTreeElement, 0)
	nodes := make([]*BTreeNode, 0)
	nodes = append(nodes, tree)
	for len(nodes) != 0 { // provera statusa elementa
		node := nodes[0]
		for i := 0; i < len(node.array); i++ {
			if node.array[i].flag == false {
				array = append(array, node.array[i])
			}
		}
		nodes = append(nodes, node.children...)
		nodes = nodes[1:]
	}
	Sort(array)
	for i := 0; i < len(array); i++ {
		fmt.Println(array[i].key)
	}
	return array
}
func Sort(niz []*BTreeElement) {
	for i := 0; i < int(len(niz)-1); i++ {
		for j := i + 1; j < int(len(niz)); j++ {
			if (niz[j]).key < (niz[i]).key {
				pom := niz[j]
				niz[j] = niz[i]
				niz[i] = pom
			}
		}
	}
}
func findTheBiggestHalfFull(node *BTreeNode) *BTreeNode {
	parent := node.parent
	if parent == nil {
		return nil
	}
	if parent.currentSize < parent.m-1 {
		return parent
	}
	return findTheBiggestHalfFull(parent)
}
func Wipe(tree *BTreeNode, el *BTreeElement) {
	node := tree
	counter := 0
	for len(node.array) > 0 {
		for i := 0; i < len(node.array); i++ {
			counter = 0
			if node.array[i] == el {
				node.array[i].flag = true
				return
			}
			if node.array[i].key > el.key {
				counter++
				node = node.children[i]
			}
		}
		if counter == 0 {
			node = node.children[len(node.array)]
		}
	}
}
func Search(tree *BTreeNode, el *BTreeElement) bool {
	node := tree
	correct := false
	if tree.currentSize == 0 {
		return false
	}
	if len(node.children) == 0 {
		for i := 0; i < int(tree.currentSize); i++ {
			if node.array[i] == el && node.array[i].flag == false {
				return true
			}
			if node.array[i] == el && node.array[i].flag == true {
				return false
			}
		}
	}
	for len(node.children) != 0 {
		for i := 0; i < len(node.array); i++ {
			if node.array[i] == el {
				return true
			}
			if node.array[i].key > el.key {
				node = node.children[i]
			}
		}
	}
	return correct
}
func FindLeaf(tree *BTreeNode, el *BTreeElement) *BTreeNode {
	node := tree
	for len(node.children) > 0 {
		brojac := 0
		for i := 0; i < len(node.array); i++ {
			if node.array[i].key > el.key && brojac == 0 {
				node = node.children[i]
				brojac++
			}
		}
		if brojac == 0 {
			node = node.children[len(node.array)]
		}
	}
	return node
}
func Insert(tree *BTreeNode, el *BTreeElement) {

	if tree.currentSize == (tree.m-1) && len(tree.children) == 0 {
		tree.array = append(tree.array, el)
		Sort(tree.array)
		middle := int64(tree.m / 2)
		podniz1 := tree.array[0:middle]
		podniz2 := tree.array[middle+1:]
		srednjiEl := tree.array[middle]
		tree.array = nil
		tree.array = append(tree.array, srednjiEl)
		d := make([]*BTreeNode, global)
		child1 := BTreeNode{tree.m, int64(len(podniz1)), podniz1, d, tree}
		child2 := BTreeNode{tree.m, int64(len(podniz2)), podniz2, d, tree}
		tree.currentSize = 1
		tree.children = append(tree.children, &child1)
		tree.children = append(tree.children, &child2)
		return
	} else {
		if tree.currentSize < (tree.m-1) && len(tree.children) == 0 {
			tree.array = append(tree.array, el)
			tree.currentSize++
			return
		}
	}
	if len(tree.children) != 0 {
		node := FindLeaf(tree, el)
		if int64(len(node.array)) < (node.m - 1) {
			var temp []*BTreeElement
			temp = append(node.array, el)
			node.array = make([]*BTreeElement, 0)
			node.array = append(node.array, temp...)

			Sort(node.array)
			node.currentSize++
		} else {
			node.array = append(node.array, el)
			Sort(node.array)
			middle := int64(node.m / 2)
			novi_el := node.array[middle]
			parent := node.parent
			if int64(len(parent.array)) < node.m-1 {
				parent.array = append(parent.array, parent.array[middle])
				parent.currentSize++
				Sort(parent.array)
				podniz := node.array[int(middle+1):]
				node.array = node.array[:middle]
				d := make([]*BTreeNode, global)
				child1 := BTreeNode{tree.m, int64(len(podniz)), podniz, d, parent}
				poz := 0
				poz = poz + 1
				counter1 := 0
				for i := 0; i < int(len(parent.array)); i++ {
					if el.key < parent.array[i].key {
						poz = i
						counter1++
						break
					}
				}
				if counter1 == 0 {
					poz++
				}
				parent.children = append(parent.children, &child1)
				for i := int(len(parent.children) - 2); i > poz-1; i-- {
					pom := parent.children[i]
					parent.children[i] = parent.children[i+1]
					parent.children[i+1] = pom
				}
				return
			} else {
				nodeStart := FindLeaf(tree, el)
				nodeBiggest := findTheBiggestHalfFull(nodeStart)

				if nodeBiggest != nil {
					for nodeStart != nodeBiggest {
						middle := int64(nodeStart.m / 2)
						parent = nodeStart.parent
						parent.array = append(parent.array, nodeStart.array[middle])
						parent.currentSize++
						Sort(parent.array)
						podniz := nodeStart.array[middle+1:]
						nodeStart.array = nodeStart.array[:middle]
						d := make([]*BTreeNode, global)
						child1 := BTreeNode{tree.m, int64(len(podniz)), podniz, d, parent}
						poz := 0
						parent.children = append(parent.children, &child1)
						change := false
						if !change {
							change = true
							counter1 := 0
							counter2 := 0
							for i := 0; i < int(len(parent.array)); i++ {
								counter2++
								if el.key < parent.array[i].key {
									poz = i
									counter1++
									break
								}
							}
							if counter1 == 0 {
								poz = int(len(parent.array)) + 1
							}
							for i := int(len(parent.children) - 2); i > poz-1; i-- {
								if parent.children[i].array[0].key > parent.children[i+1].array[0].key {
									pom := parent.children[i]
									parent.children[i] = parent.children[i+1]
									parent.children[i+1] = pom
								}

							}
							if counter2 == 1 {
								pom := parent.children[0]
								parent.children[0] = parent.children[1]
								parent.children[1] = pom
							}
						}
						Sort(parent.array)
						Sort(nodeStart.array)
						nodeStart = nodeStart.parent
					}
					tree.m = parent.m
					tree.array = parent.array
					tree.currentSize = parent.currentSize
					tree.parent = parent.parent
					niz := parent.children

					for i := 0; i < len(niz)-1; i++ {
						for j := i + 1; j < len(niz); j++ {
							nodeP := niz[i]
							nodeD := niz[j]
							if nodeP.array[0].key > nodeD.array[0].key {
								pom := niz[i]
								niz[i] = niz[j]
								niz[j] = pom
							}
						}
					}
					tree.children = niz
				} else {
					for nodeStart.parent != nil {
						srednji := int64(nodeStart.m / 2)
						parent := nodeStart.parent
						var temp []*BTreeElement
						temp = append(parent.array, novi_el)
						parent.array = make([]*BTreeElement, 0)
						parent.array = append(parent.array, temp...)
						parent.currentSize++
						Sort(parent.array)
						podniz := nodeStart.array[:srednji]
						nodeStart.array = nodeStart.array[srednji+1:]
						d := make([]*BTreeNode, global)
						l := make([]*BTreeNode, global)
						f := nodeStart.children
						for i := 0; i < len(f); i++ {
							for j := 0; j < len(f[i].children); j++ {
								for k := 0; k < len(f[i].children[j].array); k++ {
									if f[i].children[j].array[k].key < novi_el.key {
										l = append(l, f[i].children[j])
									} else {
										d = append(l, f[i].children[j])
									}
								}
							}
						}
						nodeStart.children = l
						child1 := BTreeNode{tree.m, int64(len(podniz)), podniz, d, parent}
						poz := 0
						parent.children = append(parent.children, &child1)
						counter1 := 0
						counter2 := 0
						for i := 0; i < int(len(parent.array)); i++ {
							counter2++
							if el.key < parent.array[i].key {
								poz = i
								counter1++
								break
							}
						}
						if counter1 == 0 {
							poz = int(len(parent.array)) + 1
						}

						for i := int(len(parent.children) - 2); i > poz-1; i-- {
							if parent.children[i].array[0].key > parent.children[i+1].array[0].key {
								pom := parent.children[i]
								parent.children[i] = parent.children[i+1]
								parent.children[i+1] = pom
							}
						}
						if counter2 == 1 {
							pom := parent.children[0]
							parent.children[0] = parent.children[1]
							parent.children[1] = pom
						}
						nodeStart = nodeStart.parent
					}
					srednji := int64(nodeStart.m / 2)
					p := nodeStart.array[srednji]
					levoNiz := nodeStart.array[:srednji]
					desnoNiz := nodeStart.array[srednji+1:]
					podniz := make([]*BTreeElement, 0)
					podniz = append(podniz, p)
					d := make([]*BTreeNode, global)
					l1 := make([]*BTreeNode, 0)
					d1 := make([]*BTreeNode, 0)
					for i := 0; i < int(len(nodeStart.array)+1); i++ {
						if i <= int(srednji) {
							l1 = append(l1, nodeStart.children[i])
						} else {
							d1 = append(d1, nodeStart.children[i])
						}
					}
					Sort(nodeStart.array)
					novi_Koren := BTreeNode{tree.m, int64(1), podniz, d, nil}
					Levo := BTreeNode{node.m, int64(len(levoNiz)), levoNiz, l1, &novi_Koren}
					Desno := BTreeNode{tree.m, int64(len(desnoNiz)), desnoNiz, d1, &novi_Koren}
					novi_Koren.children = append(novi_Koren.children, &Levo)
					novi_Koren.children = append(novi_Koren.children, &Desno)
					for i := 0; i < int(len(l1)); i++ {
						l1[i].parent = &Levo
					}
					for i := 0; i < int(len(d1)); i++ {
						(d1[i]) = &Desno
					}
					tree.m = novi_Koren.m
					tree.array = novi_Koren.array
					tree.parent = novi_Koren.parent
					tree.children = novi_Koren.children
					tree.currentSize = novi_Koren.currentSize
				}
			}
		}
	}
}
func CreateB(m int64) *BTreeNode {
	niz := make([]*BTreeElement, global)
	d := make([]*BTreeNode, global)
	koren := BTreeNode{m, 0, niz, d, nil}
	return &koren
}
