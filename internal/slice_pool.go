// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package internal

import (
	"sync"
)

// byteSlices pool to reduce allocations and garbace collection
var byteSlices = sync.Pool{
	// New is called when a new instance is needed
	New: func() interface{} {
		return make([]byte, 0, 20000000)
	},
}

// GetByteSlice fetches a []byte from the pool
func GetByteSlice() []byte {
	return byteSlices.Get().([]byte)
}

// PutByteSlice returns a []byte to the pool
func PutByteSlice(slice []byte) {
	slice = slice[:0]
	byteSlices.Put(slice)
}
