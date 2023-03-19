package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"aliher/tryraft/impl"
	"aliher/tryraft/repl"
	"aliher/tryraft/threaddump"

	"github.com/c-bata/go-prompt"
	"github.com/coreos/etcd/raft/raftpb"
)

// TODO:
// core
// - drop messages (real world simulation)
// - kill node (
// - restart node
// - add new node
// - remove node
// - snapshotting to truncate log
// - auto tick till next event
// - converge after op?
// enhancements
// - better completion
// - argument flags
// - colors for prompt?

type Repl struct {
	setup   bool
	cluster *impl.Cluster
	inbox   map[uint64][]raftpb.Message

	propResults chan impl.Proposal
	posted      int

	hist []string
}

func (*Repl) completer(in prompt.Document) []prompt.Suggest {
	commandsSuggest := []prompt.Suggest{
		{Text: "init", Description: "Create initial group membership list"},
		{Text: "add", Description: "Add a node to the cluster <id> <via>"},
		{Text: "start", Description: "Start raft node"},
		{Text: "tick", Description: "Advance node(s) virtual time"},
		{Text: "process", Description: "Process raft Ready message"},
		{Text: "propose", Description: "Propose next value via node"},
		{Text: "receive", Description: "Consume pending messages from other nodes"},
		{Text: "drop", Description: "Drop messages from node to node"},
		{Text: "next", Description: "Do next sensible action: either process, receive or tick till process is needed"},
		{Text: "campaign", Description: "Try to become a leader"},
		{Text: "exit", Description: "Terminate program"},
	}
	commandArgs := map[string][]prompt.Suggest{
		"drop": {
			{Text: "from", Description: "Filter messages from nodes"},
			{Text: "to", Description: "Filter messages to nodes"},
		},
	}
	parts := strings.Split(in.CurrentLineBeforeCursor(), " ")
	s := commandsSuggest
	if len(parts) > 1 {
		cmd := parts[0]
		s = commandArgs[cmd]
	}
	return prompt.FilterHasPrefix(s, in.GetWordBeforeCursor(), true)
}

const historyFile = "/tmp/repl.history"

func iterFile(filename string, handler func(line string) error) error {
	reader, err := os.Open(filename)
	if err == nil {
		defer reader.Close()
		br := bufio.NewReader(reader)
		var line string
		for line, err = br.ReadString('\n'); err == nil; line, err = br.ReadString('\n') {
			if len(line) > 1 {
				// Remove delimiter.
				err = handler(line[:len(line)-1])
				if err != nil {
					return err
				}
			}
		}
		if err == io.EOF {
			if len(line) > 1 {
				err = handler(line[:len(line)-1])
			}
			err = nil
		}
	}
	return err
}

func (r *Repl) init() {
	if err := iterFile(historyFile, func(line string) error {
		r.hist = append(r.hist, line[:len(line)-1])
		return nil
	}); err != nil {
		fmt.Printf("Failed to read history: %v\n", err)
	}
	r.propResults = make(chan impl.Proposal, 100)
	r.inbox = make(map[uint64][]raftpb.Message)
}

func (r *Repl) run() {
	prev := ""
	if len(r.hist) > 0 {
		prev = r.hist[len(r.hist)-1]
	}
	showState := true
	for {
		//fmt.Print(r.propStatus())
		if showState {
			fmt.Println(r.fullClusterStatus())
		}
		line := prompt.Input(">>> ", r.completer, prompt.OptionHistory(r.hist))
		showState = true
		ok, err := r.processInput(line)
		if !ok {
			return
		}
		if err != nil {
			fmt.Printf("error: %s\n", err.Error())
			showState = false
		}
		if line != prev {
			r.hist = append(r.hist, line)
			if hist, err := os.OpenFile(historyFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
				fmt.Fprintf(hist, "%s\n", line)
				hist.Close()
			} else {
				fmt.Printf("Failed %s\n", err.Error())
			}
			prev = line
		}
		if err := waitForIdle(); err != nil {
			fmt.Printf("Internal raft goroutines didn't finish: %s\n", err)
		}
	}
}

const (
	RAFT_STATE = 0
	INBOX      = 1
	AGREED_LOG = 2
)

type status [][]string

func (s status) width() int {
	width := 0
	for _, section := range s {
		for _, line := range section {
			if width < len(line) {
				width = len(line)
			}
		}
	}
	return width
}

func (s status) line(sectionNum int, lineNum int, width int) (string, bool) {
	if sectionNum >= len(s) || lineNum >= len(s[sectionNum]) {
		return strings.Repeat(" ", width), false
	}
	line := s[sectionNum][lineNum]
	if len(line) > width {
		return line[:width], true
	}
	return line + strings.Repeat(" ", width-len(line)), true
}

func (s *status) append(sectionNum int, line string) {
	for i := len(*s); i <= sectionNum; i++ {
		*s = append(*s, nil)
	}
	(*s)[sectionNum] = append((*s)[sectionNum], line)
}

func (r *Repl) fullClusterStatus() string {
	var allLines []string

	var ss []status
	// r.goro()
	r.cluster.Visit(func(n *impl.ExampleRaft) error {
		var s status
		s.append(RAFT_STATE, "State")
		rs := n.Status()
		s.append(RAFT_STATE, fmt.Sprintf(" ID:      %d", n.ID()))
		s.append(RAFT_STATE, fmt.Sprintf(" Term:    %d", rs.Term))
		s.append(RAFT_STATE, fmt.Sprintf(" Commit:  %d", rs.Commit))
		s.append(RAFT_STATE, fmt.Sprintf(" Applied: %d", rs.Applied))
		s.append(RAFT_STATE, fmt.Sprintf(" State:   %s", rs.RaftState.String()[5:]))
		s.append(RAFT_STATE, fmt.Sprintf(" Process? %t", n.HasMessage()))

		s.append(INBOX, "Inbox")
		for _, m := range r.inbox[n.ID()] {
			s.append(INBOX, fmt.Sprintf(" %d : %s", m.From, m.Type))
		}

		s.append(AGREED_LOG, "Consensus")
		for _, l := range n.DataLog {
			s.append(AGREED_LOG, " "+l)
		}

		ss = append(ss, s)
		return nil
	})

	maxWidth := 0
	for _, s := range ss {
		if w := s.width(); maxWidth < w {
			maxWidth = w
		}
	}
	for sid := 0; sid <= AGREED_LOG; sid++ {
		for liNum := 0; ; liNum++ {
			hasData := false
			fullLine := ""
			for _, s := range ss {
				line, data := s.line(sid, liNum, maxWidth+2)
				fullLine = fullLine + line
				hasData = hasData || data
			}
			if !hasData {
				break
			}
			allLines = append(allLines, fullLine)
		}
	}

	// Pending action missing
	return strings.Join(allLines, "\n")
}

func (r *Repl) processInput(line string) (bool, error) {
	cmd := repl.ParseCmd(line)
	if cmd.IsEmpty() {
		return true, nil
	}
	var err error
	switch cmd.Cmd {
	case "init":
		err = r.initCluster(cmd)
	case "start":
		err = r.startCmd(cmd)
	case "add":
		err = r.addCmd(cmd)
	case "tick":
		err = r.tickCmd(cmd)
	case "process":
		err = r.processCmd(cmd)
	case "propose":
		err = r.proposeCmd(cmd)
	case "receive":
		err = r.receiveCmd(cmd)
	case "campaign":
		err = r.campaignCmd(cmd)
	case "drop":
		err = r.dropCmd(cmd)
	case "next":
		err = r.nextCmd(cmd)
	case "go":
		r.goro()
	case "exit":
		return false, nil
	default:
		err = errors.New(fmt.Sprintf("error: unknown command %s", cmd))
	}
	return true, err
}

func (r *Repl) propStatus() string {
	b := strings.Builder{}
out:
	for {
		select {
		case p := <-r.propResults:
			b.WriteString(p.String())
			b.WriteString("\n")
			r.posted--
			if p.PeerId() > 0 {
				r.cluster.AddPeer(p.PeerId())
			}
		default:
			break out
		}
	}
	if r.posted > 0 {
		b.WriteString(fmt.Sprintf("Pending proposals: %d\n", r.posted))
	}
	return b.String()
}

func (r *Repl) initCluster(c repl.Cmd) error {
	if r.setup {
		return errors.New("cluster peers already initialized")
	}
	nodes, err := r.parseNodeIds(c)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.New("need at least 1 node")
	}
	r.cluster.Init(nodes...)
	r.setup = true
	return nil
}

func (r *Repl) startCmd(c repl.Cmd) error {
	nodes, err := r.parseNodeIds(c)
	if err != nil {
		return err
	}
	for _, nodeID := range nodes {
		store := r.cluster.GetStore(nodeID)
		// Break out of loop on first error. Oh well.
		if err := r.cluster.AddNode(impl.NewRaft(nodeID, store, r.cluster.Peers()...)); err != nil {
			return err
		}
		r.inbox[nodeID] = nil
	}
	return nil
}

func (r *Repl) runOnArgNodes(c repl.Cmd, orAll bool, f func(nodeId uint64) error) error {
	nodes, err := r.parseNodeIds(c)
	if err != nil {
		return err
	}
	if err = r.checkNodesExist(nodes); err != nil {
		return err
	}
	if len(nodes) == 0 && orAll {
		nodes = r.cluster.Nodes()
	}
	for _, node := range nodes {
		if err := f(node); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repl) processCmd(c repl.Cmd) error {
	return r.runOnArgNodes(c, true, func(nodeId uint64) error {
		messages := r.cluster.Node(nodeId).ProcessMessage()
		if messages != nil {
			// Route messages by destination
			for _, m := range messages {
				r.inbox[m.To] = append(r.inbox[m.To], m)
			}
		}
		//fmt.Printf("Node %d, returned %d messages\n", nodeId, len(messages))
		return nil
	})
}

// node text
func (r *Repl) proposeCmd(c repl.Cmd) error {
	if len(c.Pos) != 2 {
		return errors.New("expected two params node and proposal data")
	}
	nodeId, err := strconv.ParseUint(c.Pos[0], 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to parse node id '%s'", c.Pos[0]))
	}
	if node := r.cluster.Node(nodeId); node != nil {
		node.Propose(c.Pos[1], r.propResults)
		r.posted++
	} else {
		fmt.Printf("error: unknown node %d\n", nodeId)
	}
	return nil
}

func (r *Repl) tickCmd(c repl.Cmd) error {
	return r.runOnArgNodes(c, true, func(nodeId uint64) error {
		r.cluster.Node(nodeId).Tick()
		return nil
	})
}

func (r *Repl) parseNodesIDsOrAll(c repl.Cmd) ([]uint64, error) {
	nodes, err := r.parseNodeIds(c)
	if err != nil {
		return nil, err
	}
	if err = r.checkNodesExist(nodes); err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		nodes = r.cluster.Nodes()
	}
	return nodes, err
}

func (r *Repl) checkNodesExist(nodes []uint64) error {
	for _, nodeId := range nodes {
		if r.cluster.Node(nodeId) == nil {
			return errors.New(fmt.Sprintf("unknown node %d", nodeId))
		}
	}
	return nil
}

func (r *Repl) parseNodeIds(c repl.Cmd) ([]uint64, error) {
	var nodes []uint64
	for _, tok := range c.Pos {
		nodeId, err := strconv.ParseUint(tok, 10, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("failed to parse node id '%s'", tok))
		}
		nodes = append(nodes, nodeId)
	}
	return nodes, nil
}

func (r *Repl) campaignCmd(c repl.Cmd) error {
	node, err := r.parseSingleNodeArgs(c)
	if err != nil {
		return err
	}
	return node.Campaign()
}

func (r *Repl) parseSingleNodeArgs(c repl.Cmd) (*impl.ExampleRaft, error) {
	if len(c.Pos) != 1 {
		return nil, errors.New("expecting single node arg")
	}
	nodeId, err := strconv.ParseUint(c.Pos[0], 10, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("failed to parse node id '%s'", c.Pos[0]))
	}
	node := r.cluster.Node(nodeId)
	if node == nil {
		return nil, errors.New(fmt.Sprintf("unknown node %d", nodeId))
	}
	return node, nil
}

func (r *Repl) receiveCmd(c repl.Cmd) error {
	return r.runOnArgNodes(c, true, func(nodeId uint64) error {
		node := r.cluster.Node(nodeId)
		for _, msg := range r.inbox[nodeId] {
			node.Receive(msg)
		}
		r.inbox[nodeId] = nil
		return nil
	})
}

func (r *Repl) addCmd(c repl.Cmd) error {
	via, err := c.GetUInt(0)
	if err != nil {
		return err
	}
	node := r.cluster.Node(via)
	if node == nil {
		return errors.New(fmt.Sprintf("node %d not found", via))
	}
	newId, err := c.GetUInt(1)
	if err != nil {
		return err
	}
	store := r.cluster.GetStore(newId)
	newNode := impl.NewRaft(newId, store)
	if err = r.cluster.AddNode(newNode); err != nil {
		return err
	}
	node.ProposeConfChange(newId, r.propResults)
	r.posted++
	return nil
}

func (r *Repl) dropCmd(c repl.Cmd) error {
	all := r.cluster.Nodes()
	from, err := c.GetUIntSliceOr("from", all)
	if err != nil {
		return err
	}
	to, err := c.GetUIntSliceOr("to", all)
	if err != nil {
		return err
	}
	for node := range r.inbox {
		if !in(node, to) {
			continue
		}
		var newBox []raftpb.Message
		for _, m := range r.inbox[node] {
			if !in(m.From, from) {
				newBox = append(newBox, m)
			}
		}
		r.inbox[node] = newBox
	}
	return nil
}

func (r *Repl) nextCmd(c repl.Cmd) error {
	if r.processPending() {
		fmt.Println("process")
		return r.processCmd(c)
	}
	for _, msgs := range r.inbox {
		if len(msgs) > 0 {
			fmt.Println("receive")
			return r.receiveCmd(c)
		}
	}
	i := 0
	for ; i < 50; i++ {
		r.tickCmd(c)
		runtime.Gosched()
		// TODO: Check if all goroutines converged
		// #	0x64ee29	github.com/coreos/etcd/raft.(*node).run+0x929	/home/ali/go/pkg/mod/github.com/coreos/etcd@v3.3.27+incompatible/raft/node.go:313
		if r.processPending() {
			break
		}
	}
	fmt.Printf("Made %d ticks\n", i)
	return nil
}

func (r *Repl) goro() {
	prof := pprof.Lookup("goroutine")
	if prof == nil {
		fmt.Println("can't print goro dump")
	}
	b := bytes.Buffer{}
	_ = prof.WriteTo(&b, 0)
}

func (r *Repl) processPending() bool {
	process := false
	r.cluster.Visit(func(n *impl.ExampleRaft) error {
		process = process || n.HasMessage()
		return nil
	})
	return process
}

func in(node uint64, nodes []uint64) bool {
	for _, n := range nodes {
		if n == node {
			return true
		}
	}
	return false
}

// waitForidle waits until all goroutines except for main (#1) will block
// on select. It it takes more than a second(-ish), error is reported
func waitForIdle() error {
	for i := 0; i < 50; i++ {
		runtime.Gosched()
		<-time.After(20 * time.Millisecond)
		if !hasRunningNonMain() {
			return nil
		}
	}
	return errors.New("goroutines are not idle")
}

var grre = regexp.MustCompile(`goroutine ([0-9]+) \[(.+?)(,.*)?\]`)

// hasRunningNonMain checks if any non main goroutines are not in select
// state. This is a proxy criteria for all running data to be processed.
func hasRunningNonMain() bool {
	buf := make([]byte, 65536)
	sz := runtime.Stack(buf, true)
	r := bytes.NewReader(buf[:sz])
	s := bufio.NewScanner(r)
	for s.Scan() {
		l := s.Text()
		m := grre.FindStringSubmatch(l)
		if m != nil {
			if m[1] != "1" && m[2] != "select" {
				fmt.Printf("Unstopped: %s\n", l)
				return true
			}
		}
	}
	return false
}

func main() {
	fmt.Println("Morning!")
	c := impl.NewCluster()
	repl := Repl{
		cluster: &c,
	}
	repl.init()
	// Replay all input files to set raft to predefined state
	if len(os.Args) > 1 {
		for _, name := range os.Args[1:] {
			if err := iterFile(name, func(line string) error {
				fmt.Printf(">>> %s\n", line)
				ok, err := repl.processInput(line)
				if !ok {
					return errors.New("repl terminated from script. huh")
				}
				if err != nil {
					fmt.Println(err.Error())
				}
				return nil
			}); err != nil {
				fmt.Printf("Failed to process input file(s): %v\n", err)
			}
		}
	}
	repl.run()
}
