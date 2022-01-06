package main

import (
	"aliher/tryraft/impl"
	"bufio"
	"errors"
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/coreos/etcd/raft/raftpb"
	"io"
	"os"
	"strconv"
	"strings"
)

// TODO:
// core
// - drop messages (real world simulation)
// - kill node (
// - restart node
// - add new node
// - remove node
// enhancements
// - better completion
// - argument flags
// - colors for prompt?

type repl struct {
	setup   bool
	cluster *impl.Cluster
	inbox   map[uint64][]raftpb.Message

	propResults chan impl.Proposal
	posted      int

	hist []string
}

func (*repl) completer(in prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "init", Description: "Create initial group membership list"},
		{Text: "add", Description: "Add a node to the cluster <id> <via>"},
		{Text: "start", Description: "Start raft node"},
		{Text: "tick", Description: "Advance node(s) virtual time"},
		{Text: "process", Description: "Process raft Ready message"},
		{Text: "propose", Description: "Propose next value via node"},
		{Text: "receive", Description: "Consume pending messages from other nodes"},
		{Text: "drop", Description: "Drop messages from node to node"},
		{Text: "campaign", Description: "Try to become a leader"},
		{Text: "exit", Description: "Terminate program"},
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

func (r *repl) init() {
	if err := iterFile(historyFile, func(line string) error {
		r.hist = append(r.hist, line[:len(line)-1])
		return nil
	}); err != nil {
		fmt.Printf("Failed to read history: %v\n", err)
	}
	r.propResults = make(chan impl.Proposal, 100)
	r.inbox = make(map[uint64][]raftpb.Message)
}

func (r *repl) run() {
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
			if hist, err := os.OpenFile(historyFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 644); err == nil {
				fmt.Fprintf(hist, "%s\n", line)
				hist.Close()
			} else {
				fmt.Printf("Failed %s\n", err.Error())
			}
			prev = line
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

func (r *repl) fullClusterStatus() string {
	var allLines []string

	ns := r.cluster.Nodes()
	var ss []status
	for _, node := range ns {
		var s status

		n := r.cluster.Node(node)
		s.append(RAFT_STATE, "State")
		rs := n.Status()
		s.append(RAFT_STATE, fmt.Sprintf(" ID:      %d", node))
		s.append(RAFT_STATE, fmt.Sprintf(" Term:    %d", rs.Term))
		s.append(RAFT_STATE, fmt.Sprintf(" Commit:  %d", rs.Commit))
		s.append(RAFT_STATE, fmt.Sprintf(" Applied: %d", rs.Commit))
		s.append(RAFT_STATE, fmt.Sprintf(" State:   %s", rs.RaftState.String()[5:]))
		s.append(RAFT_STATE, fmt.Sprintf(" Process? %t", n.HasMessage()))

		s.append(INBOX, "Inbox")
		for _, m := range r.inbox[node] {
			s.append(INBOX, fmt.Sprintf(" %d : %s", m.From, m.Type))
		}

		s.append(AGREED_LOG, "Consensus")
		for _, l := range n.DataLog {
			s.append(AGREED_LOG, " "+l)
		}

		ss = append(ss, s)
	}

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

type cmd struct {
	cmd   string
	named map[string]string
	pos   []string
}

func parse(line string) cmd {
	result := cmd{
		named: make(map[string]string),
	}
	parts := strings.Fields(line)
	if len(parts) > 0 {
		result.cmd = parts[0]
		for _, part := range parts[1:] {
			named := strings.Split(part, "=")
			if len(named) > 1 {
				result.named[named[0]] = part[len(named[0])+1:]
			} else {
				result.pos = append(result.pos, part)
			}
		}
	}
	return result
}

func (c cmd) empty() bool {
	return len(c.cmd) == 0
}

func (c cmd) getUInt(i interface{}) (uint64, error) {
	var val string
	switch key := i.(type) {
	case string:
		var ok bool
		val, ok = c.named[key]
		if !ok {
			return 0, errors.New(fmt.Sprintf("parameter %s was not defined", key))
		}
	case int:
		if key < 0 || key > len(c.pos) {
			return 0, errors.New(fmt.Sprintf("not enough parameters. %d provided, %d reading", len(c.pos), key))
		}
		val = c.pos[key]
	default:
		panic("can only read indexed and named arguments")
	}
	return strconv.ParseUint(val, 10,64)
}

func (c cmd) getUIntOr(i interface{}, def uint64) (uint64, error) {
	var val string
	switch key := i.(type) {
	case string:
		var ok bool
		val, ok = c.named[key]
		if !ok {
			return def, nil
		}
	case int:
		if key < 0 || key > len(c.pos) {
			return def, nil
		}
		val = c.pos[key]
	default:
		panic("can only read indexed and named arguments")
	}
	return strconv.ParseUint(val, 10,64)
}

func (r *repl) processInput(line string) (bool, error) {
	cmd := parse(line)
	if cmd.empty() {
		return true, nil
	}
	var err error
	switch cmd.cmd {
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
	case "exit":
		return false, nil
	default:
		err = errors.New(fmt.Sprintf("error: unknown command %s", cmd))
	}
	return true, err
}

func (r *repl) propStatus() string {
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

func (r *repl) initCluster(c cmd) error {
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

func (r *repl) startCmd(c cmd) error {
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

func (r *repl) runOnArgNodes(c cmd, orAll bool, f func(nodeId uint64) error) error {
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

func (r *repl) processCmd(c cmd) error {
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
func (r *repl) proposeCmd(c cmd) error {
	if len(c.pos) != 2 {
		return errors.New("expected two params node and proposal data")
	}
	nodeId, err := strconv.ParseUint(c.pos[0], 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to parse node id '%s'", c.pos[0]))
	}
	if node := r.cluster.Node(nodeId); node != nil {
		node.Propose(c.pos[1], r.propResults)
		r.posted++
	} else {
		fmt.Printf("error: unknown node %d\n", nodeId)
	}
	return nil
}

func (r *repl) tickCmd(c cmd) error {
	return r.runOnArgNodes(c, true, func(nodeId uint64) error {
		r.cluster.Node(nodeId).Tick()
		return nil
	})
}

func (r *repl) parseNodesIDsOrAll(c cmd) ([]uint64, error) {
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

func (r *repl) checkNodesExist(nodes []uint64) error {
	for _, nodeId := range nodes {
		if r.cluster.Node(nodeId) == nil {
			return errors.New(fmt.Sprintf("unknown node %d", nodeId))
		}
	}
	return nil
}

func (r *repl) parseNodeIds(c cmd) ([]uint64, error) {
	var nodes []uint64
	for _, tok := range c.pos {
		nodeId, err := strconv.ParseUint(tok, 10, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("failed to parse node id '%s'", tok))
		}
		nodes = append(nodes, nodeId)
	}
	return nodes, nil
}

func (r *repl) campaignCmd(c cmd) error {
	node, err := r.parseSingleNodeArgs(c)
	if err != nil {
		return err
	}
	return node.Campaign()
}

func (r *repl) parseSingleNodeArgs(c cmd) (*impl.ExampleRaft, error) {
	if len(c.pos) != 1 {
		return nil, errors.New("expecting single node arg")
	}
	nodeId, err := strconv.ParseUint(c.pos[0], 10, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("failed to parse node id '%s'", c.pos[0]))
	}
	node := r.cluster.Node(nodeId)
	if node == nil {
		return nil, errors.New(fmt.Sprintf("unknown node %d", nodeId))
	}
	return node, nil
}

func (r *repl) receiveCmd(c cmd) error {
	return r.runOnArgNodes(c, true, func(nodeId uint64) error {
		node := r.cluster.Node(nodeId)
		for _, msg := range r.inbox[nodeId] {
			node.Receive(msg)
		}
		r.inbox[nodeId] = nil
		return nil
	})
}

func (r *repl) addCmd(c cmd) error {
	via, err := c.getUInt(0)
	if err != nil {
		return err
	}
	node := r.cluster.Node(via)
	if node == nil {
		return errors.New(fmt.Sprintf("node %d not found", via))
	}
	newId, err := c.getUInt(1)
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

func main() {
	fmt.Println("Morning!")
	c := impl.NewCluster()
	repl := repl{
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

func main2() {
	cmd := parse("exit 1 2 t=3 zb=12=3")
	fmt.Printf("%v\n", cmd)
}
