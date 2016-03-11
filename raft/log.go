package raft

import (
	"sync"
	"container/list"
	"time"
)

// ------------ QUORUM POLICY ------------

// NOTE: The interface below is used to allow different logFutures to have different
// commitment rules while still using the inflight mechanism. This necessity comes
// from the fact the the cluster might be partitioned.
type quorumPolicy interface {
	// NOTE: checks whether a commit for a given Raft is enough to
	// satisfy the commitment rules.
	Commit() bool

	// NOTE: checks if an operation has been committed.
	IsCommitted() bool
}

type majorityQuorum struct {
	count       int
	votesNeeded int
}

func newMajorityQuorum(clusterSize int) *majorityQuorum {
	votesNeeded := (clusterSize / 2) + 1
	return &majorityQuorum{count: 0, votesNeeded: votesNeeded}
}

func (m *majorityQuorum) Commit() bool {
	m.count++
	return m.IsCommitted()
}

func (m *majorityQuorum) IsCommitted() bool {
	return m.count >= m.votesNeeded
}

// ------------ END QUORUM POLICY ------------

type Log struct {
	Index   uint64
	Term    uint64
	Command interface{}
}

type logFuture struct {
	log      Log
	policy   quorumPolicy
	dispatch time.Time
}

func (l *logFuture) Index() uint64 {
	return l.log.Index
}

type inflight struct {
	sync.Mutex
	committed  *list.List
	commitCh   chan struct{}
	minCommit  uint64
	maxCommit  uint64
	operations map[uint64]*logFuture
}

func newInflight(commitCh chan struct{}) *inflight {
	return &inflight{
		committed:        list.New(),
		commitCh:        commitCh,
		minCommit:        0,
		maxCommit:        0,
		operations:        make(map[uint64]*logFuture),
	}
}

func (i *inflight) Start(l *logFuture) {
	i.Lock()
	defer i.Unlock()
	i.start(l)
}

func (i *inflight) StartAll(logs []*logFuture) {
	i.Lock()
	defer i.Unlock()
	for _, l := range logs {
		i.start(l)
	}
}

func (i *inflight) start(l *logFuture) {
	idx := l.log.Index
	i.operations[idx] = l

	if idx > i.maxCommit {
		i.maxCommit = idx
	}
	if i.minCommit == 0 {
		i.minCommit = idx
	}
	i.commit(idx)
}

func (i *inflight) commit(index	 uint64) {
	op, ok := i.operations[index]
	if !ok {
		// Ignore if not in the map, as it may be committed already
		printOut("[ERR] Raft: commit: operation not found")
		return
	}

	// Check if we've satisfied the commit
	//if !op.policy.Commit() {
	//	printOut("[ERR] Raft: commit: commit not satisfied")
	//	return
	//}

	// Cannot commit if this is not the minimum inflight. This can happen
	// if the quorum size changes, meaning a previous commit requires a larger
	// quorum that this commit. We MUST block until the previous log is committed,
	// otherwise logs will be applied out of order.
	if index != i.minCommit {
		return
	}

	NOTIFY:
	// Add the operation to the committed list
	i.committed.PushBack(op)

	// Stop tracking since it is committed
	delete(i.operations, index)

	// Update the indexes
	if index == i.maxCommit {
		i.minCommit = 0
		i.maxCommit = 0

	} else {
		i.minCommit++
	}

	// Check if the next in-flight operation is ready
	if i.minCommit != 0 {
		op = i.operations[i.minCommit]
		if op.policy.IsCommitted() {
			index = i.minCommit
			goto NOTIFY
		}
	}

	// Async notify of ready operations
	asyncNotifyCh(i.commitCh)
}

// Cancel is used to cancel all in-flight operations.
func (i *inflight) Cancel() {
	// Lock after close to avoid deadlock
	i.Lock()
	defer i.Unlock()

	// Clear the map
	i.operations = make(map[uint64]*logFuture)

	// Clear the list of committed
	i.committed = list.New()

	// Close the commmitCh
	close(i.commitCh)

	// Reset indexes
	i.minCommit = 0
	i.maxCommit = 0
}

// Committed returns all the committed operations in order.
func (i *inflight) Committed() (l *list.List) {
	i.Lock()
	l, i.committed = i.committed, list.New()
	i.Unlock()
	return l
}

// Commit is used by leader replication routines to indicate that
// a follower was finished committing a log to disk.
func (i *inflight) Commit(index uint64) {
	i.Lock()
	defer i.Unlock()
	i.commit(index)
}

// CommitRange is used to commit a range of indexes inclusively.
// It is optimized to avoid commits for indexes that are not tracked.
func (i *inflight) CommitRange(minIndex, maxIndex uint64) {
	i.Lock()
	defer i.Unlock()

	// Update the minimum index
	minIndex = max(i.minCommit, minIndex)

	// Commit each index
	for idx := minIndex; idx <= maxIndex; idx++ {
		i.commit(idx)
	}
}

type LogStore struct {
	l         sync.RWMutex
	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]*Log
}

func NewLogStore() *LogStore {
	return &LogStore{
		logs:        make(map[uint64]*Log),
	}
}

func (i *LogStore) FirstIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.lowIndex, nil
}

// LastIndex implements the LogStore interface.
func (i *LogStore) LastIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.highIndex, nil
}

// GetLog implements the LogStore interface.
// Returns true if log exists; false otherwise
func (i *LogStore) GetLog(index uint64, log *Log) bool {
	i.l.RLock()
	defer i.l.RUnlock()
	l, ok := i.logs[index]
	if !ok {
		return false
	}
	*log = *l
	return true
}

// StoreLog implements the LogStore interface.
func (i *LogStore) StoreLog(log *Log) error {
	return i.StoreLogs([]*Log{log})
}

// StoreLogs implements the LogStore interface.
func (i *LogStore) StoreLogs(logs []*Log) error {
	i.l.Lock()
	defer i.l.Unlock()
	for _, l := range logs {
		i.logs[l.Index] = l
		if i.lowIndex == 0 {
			i.lowIndex = l.Index
		}
		if l.Index > i.highIndex {
			i.highIndex = l.Index
		}
	}
	return nil
}

// DeleteRange implements the LogStore interface.
func (i *LogStore) DeleteRange(min, max uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	for j := min; j <= max; j++ {
		delete(i.logs, j)
	}
	i.lowIndex = max + 1
	return nil
}
