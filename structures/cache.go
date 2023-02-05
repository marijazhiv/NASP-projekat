//Dusica Trbovic, SV 42/2021
package structures

import (
	"fmt"
)

type Cache_Node struct {
	Key      string
	Value    []byte
	Next     *Cache_Node
	Previous *Cache_Node
}

type Linked_List struct {
	size         int
	head         *Cache_Node
	tail         *Cache_Node
	maximum_size int
}

type Cache struct {
	list *Linked_List
	data map[string][]byte
}

func Create_Node(key string, value []byte) *Cache_Node {
	n := Cache_Node{
		Key:   key,
		Value: value,
		Next:  nil,
	}
	return &n
}

func Create_Cache(maximum int) *Cache {
	list := Linked_List{
		size:         0,
		head:         nil,
		tail:         nil,
		maximum_size: maximum,
	}

	data := make(map[string][]byte)

	cache := Cache{
		list: &list,
		data: data,
	}

	return &cache
}

func (cache *Cache) Add_Node(key string, value []byte) {
	list := cache.list
	node := Create_Node(key, value)

	_, p := cache.data[node.Key]
	if p != false {
		c := list.head
		if c.Key == node.Key {
			delete(cache.data, node.Key)
			cache.data[node.Key] = node.Value
			return
		}
		for c.Next.Key != node.Key {
			c = c.Next
		}
		if node.Key == list.tail.Key {
			list.tail = list.tail.Previous
		}

		c_previous := c
		c = c.Next
		c_next := c.Next
		c.Next = nil
		head := list.head
		list.head = c
		head.Previous = list.head
		list.head.Next = head
		list.head.Previous = nil
		c_previous.Next = c_next
		if c_next != nil {
			c_next.Previous = c_previous
		}

		delete(cache.data, node.Key)
		cache.data[node.Key] = node.Value
		return
	}

	cache.data[node.Key] = node.Value

	if list.size == list.maximum_size {
		head := list.head
		list.head = node
		head.Previous = list.head
		list.head.Next = head
		list.tail = list.tail.Previous

		delete(cache.data, list.tail.Next.Key)
		list.head.Previous = nil
		list.tail.Next = nil
	} else {
		if list.head == nil {
			list.head = node
			list.tail = node
			list.size = list.size + 1
		} else {
			head := list.head
			list.head = node
			head.Previous = list.head
			list.head.Next = head
			list.head.Previous = nil
			list.size = list.size + 1
		}
	}
}

func (cache *Cache) Print_Cache() {
	list := cache.list
	fmt.Println("List: ")
	c := list.head
	fmt.Println(c.Key)

	for c.Next != nil {
		fmt.Println(c.Next.Key)
		c = c.Next
	}

	fmt.Println("Map: ")
	data := cache.data
	for key, value := range data {
		fmt.Println("Key: ", key, "   Value: ", string(value))
	}
}

func (cache *Cache) Delete_Node(key string) bool {
	_, p := cache.data[key]
	list := cache.list

	if p == true {
		delete(cache.data, key)
		c := list.head
		if c.Key == key {
			list.head = c.Next
			if list.head != nil {
				list.head.Previous = nil
			}
			list.size = list.size - 1
			return true //uspesno obrisan Node sa kljucem key
		}

		previous := c
		c = c.Next
		next := c.Next
		for c != nil {
			if c.Key == key {
				if next != nil {
					previous.Next = next
					next.Previous = previous
				} else {
					previous.Next = nil
					list.tail = previous
				}
				list.size = list.size - 1
				return true
			}

			previous = c
			c = c.Next
			next = c.Next
		}
	}
	return false
}

func (cache *Cache) Get_Node(key string) (bool, []byte) {
	_, p := cache.data[key]
	if p == true {
		return true, cache.data[key]
	}
	return false, nil
}
