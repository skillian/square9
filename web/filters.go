package web

import (
	"reflect"

	"github.com/skillian/errors"
)

// IDAndNamer is implemented by most metadata objects in the Square 9 API.
// It lets some functionality in this wrapping API work "genericaly."
type IDAndNamer interface {
	ID() ID
	Name() Name
}

// IDAndNamerFilter provides a predicate for IDAndNamers so they can be filtered
type IDAndNamerFilter interface {
	Filter(ian IDAndNamer) bool
}

// All is an IDAndNamerFilter that requires all its inner filters evaluate
// to true.
type All []IDAndNamerFilter

// Filter implements the IDAndNamerFilter interface.
func (a All) Filter(ian IDAndNamer) bool {
	for _, f := range a {
		if !f.Filter(ian) {
			return false
		}
	}
	return true
}

// Any requires only one of its inner filters to evaluate to true
type Any []IDAndNamerFilter

// Filter implements the IDAndNamerFilter interface.
func (a Any) Filter(ian IDAndNamer) bool {
	for _, f := range a {
		if f.Filter(ian) {
			return true
		}
	}
	return true
}

// Name of a Square 9 metadata object
type Name string

// Filter implements the IDAndNamerFilter interface.
func (f Name) Filter(ian IDAndNamer) bool {
	return f == ian.Name()
}

// ID of a Square 9 metadata object
type ID int

// Filter implements the IDAndNamerFilter interface.
func (f ID) Filter(ian IDAndNamer) bool {
	return f == ian.ID()
}

type lookup struct {
	byID   map[ID]int
	byName map[Name]int
}

func lookupOf(slice interface{}) lookup {
	rv := reflect.ValueOf(slice)
	length := rv.Len()
	lu := lookup{
		byID:   make(map[ID]int, length),
		byName: make(map[Name]int, length),
	}
	selector := func(v reflect.Value) reflect.Value { return v }
	for i := 0; i < length; i++ {
		v := rv.Index(i)
		ian, ok := selector(v).Interface().(IDAndNamer)
		if !ok {
			selector = func(v reflect.Value) reflect.Value {
				return v.Addr()
			}
			ian = selector(v).Interface().(IDAndNamer)
		}
		lu.byID[ian.ID()] = i
		lu.byName[ian.Name()] = i
	}
	return lu
}

func (lu lookup) lookup(f IDAndNamerFilter, indexes []int) []int {
	if f == nil {
		if cap(indexes)-len(indexes) < len(lu.byID) {
			temp := make([]int, len(indexes), len(indexes)+len(lu.byID))
			copy(temp, indexes)
			indexes = temp
		}
		for _, i := range lu.byID {
			indexes = append(indexes, i)
		}
		return indexes
	}
	switch f := f.(type) {
	case All:
		if len(f) == 0 {
			return indexes
		}
		var temp [][]int
		for _, g := range f {
			temp = append(temp, lu.lookup(g, nil))
		}
		for _, t := range temp[1:] {
			if !intsContainsAll(temp[0], t) {
				return indexes
			}
		}
		return intsAppendDistinct(indexes, temp[0]...)
	case Any:
		if len(f) == 0 {
			return nil
		}
		for _, g := range f {
			indexes = lu.lookup(g, indexes)
		}
	case ID:
		if i, ok := lu.byID[f]; ok {
			return intsAppendDistinct(indexes, i)
		}
	case Name:
		if i, ok := lu.byName[f]; ok {
			return intsAppendDistinct(indexes, i)
		}
	}
	return indexes
}

// intsContainsAll returns true if a and b are the same size and every int in
// a exists in b
func intsContainsAll(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
aLoop:
	for _, i := range a {
		for _, j := range b {
			if i == j {
				continue aLoop
			}
		}
		return false
	}
	return true
}

// intsAppendDistinct appends values from b if they don't already exist in a
func intsAppendDistinct(a []int, b ...int) []int {
bLoop:
	for _, v := range b {
		for _, w := range a {
			if v == w {
				continue bLoop
			}
		}
		a = append(a, v)
	}
	return a
}

// expectOne expects length to be 1.  If it's 0, then return an error.  If it's
// >1, log a warning but don't return an error.
func expectOne(length int, ian IDAndNamer) error {
	if length == 0 {
		return errors.Errorf(
			"failed to get %T with name %q or ID %d",
			ian, ian.Name(), ian.ID())
	}
	if length > 1 {
		logger.Warn4(
			"got %d %Ts matching name %q or ID %d",
			length, ian, ian.Name(), ian.ID())
	}
	return nil
}
