package impl

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"sort"
	"strings"
)

// ExampleRaft wraps real raft node hiding nitty-gritty interactions
type ExampleRaft struct {
	id   uint64
	node raft.Node

	// "Disk" storage
	storage *raft.MemoryStorage

	// Peek functionality for raft channel
	preview *raft.Ready

	// Data log that is the result of consensus
	DataLog []string
}

func NewRaft(nodeId uint64, storage *raft.MemoryStorage, peers ...uint64) *ExampleRaft {
	raftCfg := &raft.Config{
		ID:              nodeId,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxInflightMsgs: 5,
		//Logger:          nil,
	}
	// Need to add other group members
	peerIDs := []raft.Peer{}
	for _, id := range peers {
		if id != nodeId+99999 {
			peerIDs = append(peerIDs, raft.Peer{ID: id})
		}
	}
	node := raft.StartNode(raftCfg, peerIDs)
	return &ExampleRaft{id: nodeId, node: node, storage: storage}
}

func RestartRaft(nodeId uint64, storage *raft.MemoryStorage) *ExampleRaft {
	raftCfg := &raft.Config{
		ID:              nodeId,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxInflightMsgs: 5,
	}
	node := raft.RestartNode(raftCfg)
	return &ExampleRaft{node: node, storage: storage}
}

func (e *ExampleRaft) Tick() {
	e.node.Tick()
}

// Receive messages from other nodes
func (e *ExampleRaft) Receive(m raftpb.Message) {
	if err := e.node.Step(context.Background(), m); err != nil {
		panic(fmt.Sprintf("failed to process raft message: %s", err.Error()))
	}
}

func (e *ExampleRaft) HasMessage() bool {
	if e.preview == nil {
		e.tryFetchMessage()
	}
	return e.preview != nil
}

func (e *ExampleRaft) ProcessMessage() []raftpb.Message {
	if e == nil {
		return nil
	}
	if e.preview == nil {
		e.tryFetchMessage()
	}
	if e.preview == nil {
		return nil
	}
	// Process raft update
	// 1. Persist state durably
	if err := e.storage.SetHardState(e.preview.HardState); err != nil {
		panic(err)
	}
	if err := e.storage.Append(e.preview.Entries); err != nil {
		panic(err)
	}
	if !raft.IsEmptySnap(e.preview.Snapshot) {
		if err := e.storage.ApplySnapshot(e.preview.Snapshot); err != nil && err != raft.ErrSnapOutOfDate {
			panic(err)
		}
	}
	// 2. "Send" messages (actually store them in pending messages buffer)
	pendingMessages := e.preview.Messages
	// 3. Process snapshot
	if !raft.IsEmptySnap(e.preview.Snapshot) {
		// ????	Load state from snapshot?
		fmt.Printf("Received snapshot. Not sure where to put it.\n")
	}
	// 4. Process committed entries
	for _, entry := range e.preview.CommittedEntries {
		switch entry.Type {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				panic("Failed to deserialize conf change")
			}
			e.node.ApplyConfChange(cc)
		case raftpb.EntryNormal:
			value := string(entry.Data)
			e.DataLog = append(e.DataLog, value)
		}
	}

	// Advance state machine internals
	e.node.Advance()
	e.preview = nil
	return pendingMessages
}

type Proposal struct {
	id      uint64
	data    string
	newPeer uint64
	fail    error
}

func (p *Proposal) String() string {
	switch {
	case p.fail != nil:
		return fmt.Sprintf("%d: %s failed: %s", p.id, p.data, p.fail)
	case p.newPeer > 0:
		return fmt.Sprintf("%d: new peer %d", p.id, p.newPeer)
	default:
		return fmt.Sprintf("%d: %s", p.id, p.data)
	}
}

func (p *Proposal) PeerId() uint64 {
	return p.newPeer
}

// Propose is blocking so we'll fire up go routing to do it.
// maybe collect results via channel? Not sure if we can have multiple, or
// we guaranteed not to err.
func (e *ExampleRaft) Propose(data string, result chan<- Proposal) {
	go func() {
		if err := e.node.Propose(context.Background(), []byte(data)); err != nil {
			result <- Proposal{
				id:   e.id,
				data: data,
				fail: err,
			}
		} else {
			result <- Proposal{
				id:   e.id,
				data: data,
			}
		}
	}()
}

func (e *ExampleRaft) String() string {
	return fmt.Sprintf("Raft{}")
}

func (e *ExampleRaft) tryFetchMessage() {
	select {
	case rd := <-e.node.Ready():
		e.preview = &rd
	default:
	}
}

func (e *ExampleRaft) Campaign() error {
	return e.node.Campaign(context.Background())
}

func (e *ExampleRaft) Status() raft.Status {
	return e.node.Status()
}

func (e *ExampleRaft) ProposeConfChange(newNodeId uint64, result chan<- Proposal) {
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddLearnerNode,
		NodeID: newNodeId,
	}
	go func() {
		if err := e.node.ProposeConfChange(context.Background(), cc); err != nil {
			result <- Proposal{
				id:   e.id,
				fail: err,
			}
		} else {
			result <- Proposal{
				id:      e.id,
				newPeer: newNodeId,
			}
		}
	}()
}

type Cluster struct {
	// Sort of disk here
	stores map[uint64]*raft.MemoryStorage

	// Peer set
	peers map[uint64]interface{}

	// All nodes of the cluster
	nodes map[uint64]*ExampleRaft
}

func NewCluster() Cluster {
	return Cluster{
		stores: map[uint64]*raft.MemoryStorage{},
		peers:  map[uint64]interface{}{},
		nodes:  map[uint64]*ExampleRaft{},
	}
}

func (c *Cluster) Init(nodes ...uint64) {
	for _, node := range nodes {
		c.peers[node] = node
	}
	fmt.Printf("added initial nodes: %v\n", c.peers)
}

func (c *Cluster) AddNode(e *ExampleRaft) error {
	if _, ok := c.nodes[e.id]; ok {
		return errors.New("can't add node twice")
	}
	// Disabled because it breaks new node add.
	// Need to rethink what peers field is for.
	// Only needed for the initial startup really.
	//if _, ok := c.peers[e.id]; !ok {
	//	return errors.New("node must be in peers")
	//}
	c.nodes[e.id] = e
	return nil
}

func (c *Cluster) Node(id uint64) *ExampleRaft {
	return c.nodes[id]
}

func (c *Cluster) GetStore(nodeID uint64) *raft.MemoryStorage {
	var store *raft.MemoryStorage
	var ok bool
	if store, ok = c.stores[nodeID]; !ok {
		store = raft.NewMemoryStorage()
		c.stores[nodeID] = store
	}
	return store
}

func (c *Cluster) Peers() (peers []uint64) {
	for id := range c.peers {
		peers = append(peers, id)
	}
	return
}

func (c *Cluster) State() string {
	var states []string
	nodeIds := c.Nodes()
	for _, id := range nodeIds {
		status := c.nodes[id].node.Status()
		states = append(states,
			fmt.Sprintf(
				"%d:t%d,c%d,a%d,%s",
				id, status.Term, status.Commit, status.Applied, status.RaftState.String()[5:]))
	}
	return strings.Join(states, "|")
}

func (c *Cluster) Nodes() []uint64 {
	var nodeIds []uint64
	for id := range c.nodes {
		nodeIds = append(nodeIds, id)
	}
	sort.Slice(nodeIds, func(i, j int) bool {
		return nodeIds[i] < nodeIds[j]
	})
	return nodeIds
}

func (c *Cluster) AddPeer(nodeId uint64) {
	c.peers[nodeId] = struct {}{}
}
