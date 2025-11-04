package gorelly

type NodeType [8]byte

var (
	NodeTypeLeaf   = NodeType{'L', 'E', 'A', 'F', ' ', ' ', ' ', ' '}
	NodeTypeBranch = NodeType{'B', 'R', 'A', 'N', 'C', 'H', ' ', ' '}
)

type NodeHeader struct {
	nodeType NodeType
}

type Node struct {
	header *NodeHeader
}

func NewLeafNode() *Node {
	return &Node{
		header: &NodeHeader{
			nodeType: NodeTypeLeaf,
		},
	}
}

func NewBranchNode() *Node {
	return &Node{
		header: &NodeHeader{
			nodeType: NodeTypeBranch,
		},
	}
}
