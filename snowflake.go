package snowflake

import (
	"fmt"
	"sync/atomic"
	"time"
)

const (
	defaultEpoch = 1288834974657
	serverBits   = 10
	sequenceBits = 12
	timeBits     = 42
	serverShift  = sequenceBits
	timeShift    = sequenceBits + serverBits
	serverMax    = ^(-1 << serverBits)
	sequenceMask = ^(-1 << sequenceBits)
	timeMask     = ^(-1 << timeBits)
)

type Snowflake struct {
	state int64
	node  int64
	epoch int64
}

func New(nodeID int) *Snowflake {
	return NewWithEpoch(nodeID, defaultEpoch)
}

func NewWithEpoch(nodeID int, epoch int64) *Snowflake {
	if nodeID < 0 || nodeID > serverMax {
		panic(fmt.Errorf("invalid machine id; must be 0 ≤ id < %d", serverMax))
	}
	return &Snowflake{
		state: 0,
		node:  int64(nodeID << serverShift),
		epoch: epoch,
	}
}

func (n *Snowflake) Node() int {
	return int(n.node >> serverShift)
}

func (n *Snowflake) ID() int64 {
	var state int64

	// we attempt 100 times to update the millisecond part of the state
	// and increment the sequence atomically. each attempt is approx ~30ns
	// so we spend around ~3µs total.
	for i := 0; i < 100; i++ {
		t := (now() - n.epoch) & timeMask
		current := atomic.LoadInt64(&n.state)
		currentTime := current >> timeShift & timeMask
		currentSeq := current & sequenceMask

		switch {
		// if our time is in the future, use that with a zero sequence number.
		case t > currentTime:
			state = t << timeShift

		// we now know that our time is at or before the current time.
		// if we're at the maximum sequence, bump to the next millisecond
		case currentSeq == sequenceMask:
			state = (currentTime + 1) << timeShift

		// otherwise, increment the sequence.
		default:
			state = current + 1
		}

		if atomic.CompareAndSwapInt64(&n.state, current, state) {
			break
		}

		state = 0
	}

	// since we failed 100 times, there's high contention. bail out of the
	// loop to bound the time we'll spend in this method, and just add
	// one to the counter. this can cause millisecond drift, but hopefully
	// some CAS eventually succeeds and fixes the milliseconds. additionally,
	// if the sequence is already at the maximum, adding 1 here can cause
	// it to roll over into the machine id. giving the CAS 100 attempts
	// helps to avoid these problems.
	if state == 0 {
		state = atomic.AddInt64(&n.state, 1)
	}

	return state | n.node
}

func now() int64 {
	return int64(time.Now().UnixNano() / 1e6)
}
