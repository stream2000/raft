/*
@Time : 2020/3/31 23:42
@Author : Minus4
*/
package mr

import (
	"container/list"
)

// 同样的接口，不一样的实现。 在大容量的时候十分有效
// The set is a simple data structure, and it is not thread-safe
type Set struct {
	index map[interface{}]*list.Element
	l     list.List
}

// Contains checks if the key is in the set
func (s *Set) Contains(key interface{}) bool {
	_, ok := s.index[key]
	return ok
}

//FetchOne fetch an item from the set
func (s *Set) FetchOne() (key interface{}) {
	if s.l.Len() == 0 {
		return
	}
	front := s.l.Front()
	key = front.Value
	delete(s.index, key)
	s.l.Remove(front)
	return
}

func (s *Set) Add(key interface{}) {
	if _, ok := s.index[key]; ok {
		return
	}
	e := s.l.PushBack(key)
	s.index[key] = e
}

func (s *Set) Delete(key interface{}) {
	if e, ok := s.index[key]; ok {
		delete(s.index, e)
		s.l.Remove(e)
	} else {
		return
	}

}

func (s *Set) Len() int {
	return s.l.Len()
}

func NewSet() *Set {
	return &Set{index: make(map[interface{}]*list.Element)}
}
