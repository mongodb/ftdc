// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bsonx

// Iterator describes the types used to iterate over a bson Document.
type Iterator interface {
	Next() bool
	Element() *Element
	Err() error
}

// ElementIterator facilitates iterating over a bson.Document.
type elementIterator struct {
	d     *Document
	index int
	elem  *Element
	err   error
}

func newIterator(d *Document) *elementIterator {
	return &elementIterator{d: d}
}

// Next fetches the next element of the document, returning whether or not the next element was able
// to be fetched. If true is returned, then call Element to get the element. If false is returned,
// call Err to check if an error occurred.
func (itr *elementIterator) Next() bool {
	if itr.index >= len(itr.d.elems) {
		return false
	}

	e := itr.d.elems[itr.index]

	_, err := e.Validate()
	if err != nil {
		itr.err = err
		return false
	}

	itr.elem = e
	itr.index++

	return true
}

// Element returns the current element of the Iterator. The pointer that it returns will
// _always_ be the same for a given Iterator.
func (itr *elementIterator) Element() *Element {
	return itr.elem
}

// Err returns the error that occurred when iterating, or nil if none occurred.
func (itr *elementIterator) Err() error {
	return itr.err
}

// readerIterator facilitates iterating over a bson.Reader.
type readerIterator struct {
	r    Reader
	pos  uint32
	end  uint32
	elem *Element
	err  error
}

// newReaderIterator constructors a new readerIterator over a given Reader.
func newReaderIterator(r Reader) (*readerIterator, error) {
	itr := new(readerIterator)
	if len(r) < 5 {
		return nil, newErrTooSmall()
	}
	givenLength := readi32(r[0:4])
	if len(r) < int(givenLength) {
		return nil, ErrInvalidLength
	}

	itr.r = r
	itr.pos = 4
	itr.end = uint32(givenLength)
	itr.elem = &Element{value: &Value{}}

	return itr, nil
}

// Next fetches the next element of the Reader, returning whether or not the next element was able
// to be fetched. If true is returned, then call Element to get the element. If false is returned,
// call Err to check if an error occurred.
func (itr *readerIterator) Next() bool {
	if itr.pos >= itr.end {
		itr.err = ErrInvalidReadOnlyDocument
		return false
	}
	if itr.r[itr.pos] == '\x00' {
		return false
	}
	elemStart := itr.pos
	itr.pos++
	n, err := itr.r.validateKey(itr.pos, itr.end)
	itr.pos += n
	if err != nil {
		itr.err = err
		return false
	}

	itr.elem.value.start = elemStart
	itr.elem.value.offset = itr.pos
	itr.elem.value.data = itr.r
	itr.elem.value.d = nil

	n, err = itr.elem.value.validate(false)
	itr.pos += n
	if err != nil {
		itr.err = err
		return false
	}
	return true
}

// Element returns the current element of the readerIterator. The pointer that it returns will
// _always_ be the same for a given readerIterator.
func (itr *readerIterator) Element() *Element {
	return itr.elem
}

// Err returns the error that occurred when iterating, or nil if none occurred.
func (itr *readerIterator) Err() error {
	return itr.err
}
