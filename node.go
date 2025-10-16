package gorelly

type NodeType [8]byte

var (
	NodeTypeLeaf   = NodeType{'L', 'E', 'A', 'F', ' ', ' ', ' ', ' '}
	NodeTypeBranch = NodeType{'B', 'R', 'A', 'N', 'C', 'H', ' ', ' '}
)

type NodeHeader struct{}

type Node struct {
	header *NodeHeader
}
