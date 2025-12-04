// Package btree provides node structures for B+ tree implementation.
package btree

import (
	"unsafe"

	"github.com/Johniel/gorelly/btree/branch"
	"github.com/Johniel/gorelly/btree/leaf"
)

// NodeHeaderSize is the size of the node header (8 bytes for node type).
const NodeHeaderSize = 8

var (
	// NodeTypeLeaf identifies a leaf node.
	NodeTypeLeaf = [8]byte{'L', 'E', 'A', 'F', ' ', ' ', ' ', ' '}
	// NodeTypeBranch identifies a branch (internal) node.
	NodeTypeBranch = [8]byte{'B', 'R', 'A', 'N', 'C', 'H', ' ', ' '}
)

// NodeHeader contains the type information for a B+ tree node.
type NodeHeader struct {
	NodeType [8]byte // Type identifier: "LEAF    " or "BRANCH  "
}

// Node represents a B+ tree node (either leaf or branch).
// It provides a unified interface for accessing node data.
type Node struct {
	header *NodeHeader
	body   []byte // Node body (leaf or branch data)
}

func NewNode(page []byte) *Node {
	if len(page) < NodeHeaderSize {
		panic("node page too small")
	}
	header := (*NodeHeader)(unsafe.Pointer(&page[0]))
	body := page[NodeHeaderSize:]
	return &Node{
		header: header,
		body:   body,
	}
}

func (n *Node) InitializeAsLeaf() {
	n.header.NodeType = NodeTypeLeaf
}

func (n *Node) InitializeAsBranch() {
	n.header.NodeType = NodeTypeBranch
}

func (n *Node) IsLeaf() bool {
	return n.header.NodeType == NodeTypeLeaf
}

func (n *Node) IsBranch() bool {
	return n.header.NodeType == NodeTypeBranch
}

func (n *Node) Body() []byte {
	return n.body
}

func (n *Node) AsLeaf() *leaf.Leaf {
	return leaf.NewLeaf(n.body)
}

func (n *Node) AsBranch() *branch.Branch {
	return branch.NewBranch(n.body)
}
