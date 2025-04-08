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
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	//"golang.org/x/text/cases"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

type AsyncRand struct {
	mtx  sync.Mutex
	Rand *rand.Rand
}

func (r *AsyncRand) Intn(n int) int {
	r.mtx.Lock()
	val := r.Rand.Intn(n)
	r.mtx.Unlock()
	return val
}

var asyncRand = &AsyncRand{
	Rand: rand.New(
		rand.NewSource(time.Now().Local().UnixNano()),
	),
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// numbers of nodes who responded this term
	live map[uint64]bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	log := newLog(c.Storage)
	if c.Applied > 0 {
		log.applied = c.Applied
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	if c.peers == nil {
		c.peers = cs.Nodes
	}
	prs := make(map[uint64]*Progress)
	for _, pr := range c.peers {
		prs[pr] = &Progress{
			Next:  0,
			Match: 0,
		}
	}

	raft := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          log,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   0,
		live:             make(map[uint64]bool),
	}

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

	return raft
}

func (r *Raft) resetRandomizedElectionTimeout() {
	val := asyncRand.Intn(r.electionTimeout)
	r.electionTimeout += val
	r.electionTimeout -= (r.electionTimeout - 10) / 10 * 10
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.leadTransferee = None
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.live = make(map[uint64]bool)
	r.live[r.id] = true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}
	preLogIndex := pr.Next - 1
	term := r.Term
	leader := r.id
	committedIndex := r.RaftLog.committed
	preLogTerm, err := r.RaftLog.Term(preLogIndex)

	if err != nil || r.RaftLog.FirstIndex()-1 > preLogIndex {
		r.sendSnapshot(to)
		return false
	}

	var entries []*pb.Entry
	for i := pr.Next; i < r.RaftLog.LastIndex()+1; i++ {
		entries = append(entries, &r.RaftLog.entries[i-r.RaftLog.FirstIndex()])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    leader,
		To:      to,
		Term:    term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: entries,
		Commit:  committedIndex,
	}

	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	term := r.Term
	_, ok := r.Prs[to]
	if !ok {
		log.Panic("Peer not founded.")
		return
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    term,
		To:      to,
		From:    r.id,
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) sendRequestVote(to uint64) {
	_, ok := r.Prs[to]
	if !ok {
		log.Panic("Peer not founded.")
		return
	}
	term := r.Term
	last := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(last)

	if err != nil {
		return
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		Term:    term,
		LogTerm: logTerm,
		Index:   last,
		From:    r.id,
		To:      to,
	}

	//log.Print("From ", r.id, " to ", to)

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendSnapshot(to uint64) {

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	r.electionElapsed++
	switch r.State {
	case StateFollower:
		//log.Printf("t= %d\n", r.electionElapsed)
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
			if err != nil {
				return
			}
		}
	case StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
			if err != nil {
				return
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		lives := len(r.live)
		total := len(r.Prs)
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			r.live = make(map[uint64]bool)
			r.live[r.id] = true

			if lives*2 <= total {
				r.startElection()
			}

			if r.leadTransferee != None {
				r.leadTransferee = None
			}
		}
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
			if err != nil {
				log.Panic("Error in sending heartbeaat.", err)
			}
			return
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower && len(r.Prs) != 1 {
		log.Panic("Invalid transition from Fllower -> Leader.")
		return
	}
	r.State = StateLeader
	r.reset(r.Term)
	r.Lead = r.id

	lastIndex := r.RaftLog.LastIndex()
	for _, pr := range r.Prs {
		pr.Next = lastIndex + 1
		pr.Match = 0
	}

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: lastIndex + 1,
	})

	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1

	for pr := range r.Prs {
		if pr != r.id {
			r.sendAppend(pr)
		}
	}

	r.updateCommitIndex()
	// NOTE: Leader should propose a noop entry on its term
}

func (r *Raft) FollwerStep(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgBeat:

	case pb.MessageType_MsgPropose:
		err = ErrProposalDropped
	case pb.MessageType_MsgHup:
		if _, ok := r.Prs[r.id]; ok {
			r.startElection()
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.startElection()
	}
	return err
}

func (r *Raft) CandidateStep(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgBeat:

	case pb.MessageType_MsgPropose:
		err = ErrProposalDropped
	case pb.MessageType_MsgHup:
		if _, ok := r.Prs[r.id]; ok {
			r.startElection()
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		total := len(r.Prs)
		agree := 0
		deny := 0
		r.votes[m.From] = !m.Reject
		for _, vote := range r.votes {
			if vote {
				agree++
			} else {
				deny++
			}
		}
		if 2*agree > total {
			r.becomeLeader()
		} else if 2*deny >= total {
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.startElection()
	}
	return err
}

func (r *Raft) LeaderStep(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for pr := range r.Prs {
			if pr != r.id {
				r.sendHeartbeat(pr)
			}
		}
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			r.handlePropose(m)
		} else {
			err = ErrProposalDropped
		}
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.startElection()
	}
	return err
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	var err error
	switch r.State {
	case StateFollower:
		err = r.FollwerStep(m)
	case StateCandidate:
		err = r.CandidateStep(m)
	case StateLeader:
		err = r.LeaderStep(m)
	}
	return err
}

func (r *Raft) updateCommitIndex() uint64 {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	maxN := match[(len(r.Prs)-1)/2]
	N := maxN
	for ; N > r.RaftLog.committed; N-- {
		if term, _ := r.RaftLog.Term(N); term == r.Term {
			break
		}
	}
	r.RaftLog.committed = N
	return r.RaftLog.committed
}

func (r *Raft) appendEntry(es []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = lastIndex + 1 + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *es[i])
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	return
}

func (r *Raft) handlePropose(m pb.Message) {
	r.appendEntry(m.Entries)
	for pr := range r.Prs {
		if pr != r.id {
			r.sendAppend(pr)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.commitTo(r.Prs[r.id].Match)
	}
}

func (r *Raft) sendRequestVoteResponse(reject bool, to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.Term < m.Term {
		r.Vote = None
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	if m.Term < r.Term {
		r.sendRequestVoteResponse(true, m.From)
		return
	}

	if r.Vote == None || r.Vote == m.From {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		if m.LogTerm > lastTerm || m.LogTerm == lastTerm && m.Index >= lastIndex {
			r.sendRequestVoteResponse(false, m.From)
			r.Vote = m.From
		} else {
			r.sendRequestVoteResponse(true, m.From)
		}
	} else {
		r.sendRequestVoteResponse(true, m.From)
	}
}

func (r *Raft) sendAppendResponse(reject bool, to uint64, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	if r.State == StateLeader {
		return
	}

	if m.Term < r.Term {
		r.sendAppendResponse(true, m.From, r.RaftLog.LastIndex())
		return
	}

	if m.From != r.Lead {
		r.Lead = m.From
	}
	preLogIndex := m.Index
	preLogTerm := m.LogTerm

	if preLogIndex > r.RaftLog.LastIndex() {
		r.sendAppendResponse(true, m.From, r.RaftLog.LastIndex())
		return
	}

	tmpTerm, _ := r.RaftLog.Term(preLogIndex)
	if tmpTerm != preLogTerm {
		r.sendAppendResponse(true, m.From, r.RaftLog.LastIndex())
		return
	}

	for _, entry := range m.Entries {
		index := entry.Index
		oldTerm, err := r.RaftLog.Term(index)
		firstIndex, _ := r.RaftLog.storage.FirstIndex()
		if index-firstIndex > uint64(len(r.RaftLog.entries)) {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else if oldTerm != entry.Term || err != nil {
			if index < firstIndex {
				r.RaftLog.entries = make([]pb.Entry, 0)
			} else {
				r.RaftLog.entries = r.RaftLog.entries[0 : index-firstIndex]
			}
			r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}

	r.RaftLog.lastAppend = m.Index + uint64(len(m.Entries))
	r.sendAppendResponse(false, m.From, r.RaftLog.LastIndex())
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.lastAppend)
	}

}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
		r.sendAppend(m.From)
		return
	}

	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	oldCommit := r.RaftLog.committed
	r.updateCommitIndex()
	if r.RaftLog.committed != oldCommit {
		for pr := range r.Prs {
			if pr != r.id {
				r.sendAppend(pr)
			}
		}
	}

	if m.From == r.leadTransferee {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: m.From})
	}
}

func (r *Raft) sendHeartbeatResponse(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	//Check term
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	if r.Lead != m.From {
		r.Lead = m.From
	}

	r.electionElapsed = 0

	r.sendHeartbeatResponse(m.From)
}

func (r *Raft) startElection() {
	if _, ok := r.Prs[r.id]; !ok {
		log.Panic("Peer not found.")
		return
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.Term++
	} else {
		r.becomeCandidate()
		for pr := range r.Prs {
			if pr != r.id {
				r.sendRequestVote(pr)
			}
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
