package structures
//Marija Zivanovic, SV19/2021

import (
    "crypto/sha1"
    "encoding/hex"
    "fmt"
    "log"
    "os"
    "strings"
)

type MerkleRoot struct {   //1. struktura MerkleRoot ima referencu na MerkleNode
    root *MerkleNode
}

func (mr *MerkleRoot) String() string {
    return mr.root.String()
}

type MerkleNode struct {
    data  [20]byte          //hash vrednost koju svaki element cuva; 
    left  *MerkleNode       //2. struktura MerkelNode koja ima reference na levo i desno dete
    right *MerkleNode
}

func (n *MerkleNode) String() string {   //metoda string vrsi enkodovanje
    return hex.EncodeToString(n.data[:])   //enkodovanje, iz tog niza bajtova u text koji je jednostavniji za citanje/obradu
}

func Hash(data []byte) [20]byte {   //niz bajtova propusta kroz f-ju Sum iz paketa sha1
    return sha1.Sum(data)     //dobijamo niz od 20 bajtova kao povratnu vrednost--> to cuvamo u MerkleNode
}

// ...

// StringsToBytes - treba u slucaju da se posalje array stringova
func StringsToBytes(strings []string) [][]byte {
    data := [][]byte{}
    for i := 0; i < len(strings); i++ {
        key_byte := []byte(strings[i])     //ako se posalje array stringova on se pretvara u array bajtova
        data = append(data, key_byte)
    }
    return data
}

// CreateMerkleTree - funkcija od koje krece kriranje stabla

//Merkle stablo se formira unazad--> bottom-up pristupom; pocinje se od dna ka vrhu

//Izgradi se prvi nivo koji se naziva listovi, tako sto se hashira svaki primljeni podatak sacuvan u data (kao bajt)

//Zatim se od prvog nivoa hashiraju zajednicke vrednosti svaka dva lista i formira se naredni cvor i sve dok se ne dodje do jednog cvora

//koji predstavlja Markle root element-> povratna vrednost ovog stabla (koren)

func CreateMerkleTree(keys [][]byte, path string) *MerkleRoot {

    // ako je prosledjen array bajtova
    data := keys

    leaves := Leaves(data)    //formira listove stabla od preuzetih podataka
    root_node := CreateAllNodes(leaves)     //kreira sve nivoe stabla od listova ka korenu (od preuzetih listova)

    root := MerkleRoot{root_node}
    new_path := strings.Replace(path, "Data.db", "Metadata.txt", 1)
    new_path = "./kv-system/data/metadata/" + new_path
    WriteInFile(root_node, new_path)
    return &root
}

// Leaves - formira listove stabla
func Leaves(data [][]byte) []*MerkleNode {
    leaves := []*MerkleNode{}

    for i := 0; i < len(data); i++ {  //kroz svaki podatak kroz koji prodjemo 
        node := MerkleNode{Hash(data[i]), nil, nil}    //nad njim pozovemo Hash funkciju
        leaves = append(leaves, &node)     //dakle svaki cvor cuva hash vrednost, tj list je (list, cvor)
    }

    return leaves
}

// CreateAllNodes - kreira sve nivoe stabla od listova ka korenu
func CreateAllNodes(leaves []*MerkleNode) *MerkleNode {

    // svi cvorovi jednog nivoa
    level := []*MerkleNode{}

    nodes := leaves

    if len(nodes) > 1 {
        for i := 0; i < len(nodes); i += 2 {  //obilazmo sve cvorove odnosno listove, onoliko koliko ih ima---> 1. nivo
            if (i + 1) < len(nodes) {
                node1 := nodes[i]    //cvor koji je na redu cuvamo u node1
                node2 := nodes[i+1]    //njegovog suseda cuvamo u node2
                node1_data := node1.data[:]  //cuvamo podatke iz 1. cvora
                node2_data := node2.data[:]   //cuvamo podatke iz drugog cvora
                new_node_bytes := append(node1_data, node2_data...)    //zajednicke vrednosti prvog i drugog cvora cuvamo u jednu promenljivu
                new_node := MerkleNode{Hash(new_node_bytes), node1, node2} //formiramo novi cvor tako sto hashiramo zajednicku vrednost para cvorova
                level = append(level, &new_node)    //novi nivo se formira tako sto se prosiri svim novoformiranim cvorovima 
            } else { // ako nam fali odgovarajuci cvor
                node1 := nodes[i]
                node2 := MerkleNode{data: [20]byte{}, left: nil, right: nil} //samo postavimo da je naredni cvor prazan
                node1_data := node1.data[:]
                node2_data := node2.data[:]
                new_node_bytes := append(node1_data, node2_data...)
                new_node := MerkleNode{Hash(new_node_bytes), node1, &node2}
                level = append(level, &new_node)
            }
        }
        nodes = level  //svi nivoi

        if len(nodes) == 1 {
            return nodes[0]
        }
    }
    return CreateAllNodes(level)
}

// PrintTree - print stablo po sirini
func PrintTree(root *MerkleNode) {
    queue := make([]*MerkleNode, 0)
    queue = append(queue, root)

    for len(queue) != 0 {
        e := queue[0]
        queue = queue[1:]
        //fmt.Println(e.String())

        if e.left != nil {
            queue = append(queue, e.left)
        }
        if e.right != nil {
            queue = append(queue, e.right)
        }
    }
}

func WriteInFile(root *MerkleNode, path string) {
    newFile, err := os.Create(path)
    err = newFile.Close()
    if err != nil {
        return
    }
    file, err := os.OpenFile(path, os.O_WRONLY, 0444)
    if err != nil {
        log.Fatal(err)
    }

    queue := make([]*MerkleNode, 0)
    queue = append(queue, root)

    for len(queue) != 0 {
        e := queue[0]
        queue = queue[1:]
        _, _ = file.WriteString(e.String() + "\n")

        if e.left != nil {
            queue = append(queue, e.left)
        }
        if e.right != nil {
            queue = append(queue, e.right)
        }
    }
    err = file.Close()
    if err != nil {
        fmt.Println(err)
    }
}

