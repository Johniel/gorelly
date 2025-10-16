package gorelly

type BranchHeader struct {
	RightChild PageID
}

type Branch struct {
	header *BranchHeader
}
