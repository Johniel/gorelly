package gorelly

type LeafHeader struct {
	PrevLeafID PageID
	NextLeafID PageID
}

type Leaf struct {
	header *LeafHeader
}

func (l *Leaf) PrevLeafID() PageID {
	return l.header.PrevLeafID
}

func (l *Leaf) NextLeafID() PageID {
	return l.header.NextLeafID
}
