package main

import (
	"sync/atomic"
)

var (
	putItemMin    uint64 = 100_000_000_000_000_000
	putItemMax    uint64 = 6_099_999_999_999_999_999
	updateItemMin uint64 = 6_100_000_000_000_000_000
	updateItemMax uint64 = 12_099_999_999_999_999_999
	endItemMin    uint64 = 12_100_000_000_000_000_000
	endItemMax    uint64 = 18_099_999_999_999_999_999
)

var (
	getItemPKManager    = newUintManager(0, numPKs)
	getItemSKManager    = newUintManager(0, numSKs)
	putItemPKManager    = newUintManager(putItemMin, putItemMax)
	putItemSKManager    = newUintManager(0, numSKs)
	updateItemPKManager = newUintManager(updateItemMin, updateItemMax)
	updateItemSKManager = newUintManager(0, numSKs)
	batchWritePKManager = newUintManager(endItemMin, endItemMax)
	batchWriteSKManager = newUintManager(0, numSKs)
)

type uintManager struct {
	mn  uint64
	mx  uint64
	crt *atomic.Uint64
}

func newUintManager(mn, mx uint64) *uintManager {
	out := &uintManager{
		mn:  mn,
		mx:  mx,
		crt: &atomic.Uint64{},
	}

	out.crt.Store(mn)

	return out
}

func (u *uintManager) next() uint64 {
	for {
		crt := u.crt.Load()

		next := crt + 1
		if next > u.mx {
			next = u.mn
		}

		if u.crt.CompareAndSwap(crt, next) {
			return next
		}
	}
}

func inArray[T comparable](arr []T, elem T) bool {
	for _, i := range arr {
		if i == elem {
			return true
		}
	}

	return false
}

func isWriteOp(op string) bool {
	writeOps := []string{"PutItem", "BatchWriteItem", "UpdateItem"}
	return inArray(writeOps, op)
}
