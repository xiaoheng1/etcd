// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"sync"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.

// 该接口简介：
// 核心内容是实现了数据在内存中的存储.

// 疑问：
// 在 JRaft 中，因为快照包数据在内存中只有一份，如果更新失败了如果处理？？？

// 存储接口，用户侧需要实现该接口.
// 目前该接口主要是存储 raft 节点临时的数据，例如 Entrys、快照等.
// 对比 JRaft:
// 在 JRaft 中，storage 存储主要分为三部分：
// 1. raft 集群元数据
// 2. entry 条目
// 3. 快照包

// 总结：
// 需要存储的数据都分为三部分：
// 1. raft 集群元数据
// 2. entry 条目
// 3. 快照包
type Storage interface {
	// TODO(tbg): split this into two interfaces, LogStorage and StateStorage.

	// 这个应该就是 Raft 集群元数据的概念
	// HardState 是需要被持久化的
	// ConfState 是集群间的配置
	// InitialState returns the saved HardState and ConfState information.
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
// 基于内存的存储实现
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		// 创建了一个切片，长度为 1
		// hardState 和 snapshot 会默认为零值.
		ents: make([]pb.Entry, 1),
	}
}

// InitialState implements the Storage interface.
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		getLogger().Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	//handle check for old snapshot being applied
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	ms.snapshot = snap
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		getLogger().Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		getLogger().Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		getLogger().Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
