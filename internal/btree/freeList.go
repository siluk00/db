package btree

// Node format
// |next	|pointers	|unused	|
// | 8B		| n*8B		| ...	|
type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

func (node LNode) getNext() uint64
func (node LNode) setNext(next uint64)
func (node LNode) getPtr(idx int) uint64
func (node LNode) setPtr(idx int, ptr uint64)

type FreeList struct {
	get         func(uint64) []byte // read a page
	newFreeList func([]byte) uint64 // apend a new page
	set         func(uint64) []byte // update a page

	// persisted data in the meta page
	headPage uint64 //pointer to list head node
	headSeq  uint64 // monotonic sequence number to index into the list head
	tailPage uint64
	tailSeq  uint64

	//in memory states
	maxSeq uint64 //saved tailSeqto prevent consuming newly added items
}

func (fl *FreeList) PopHead() uint64 {
	ptr, head := flPop(fl)
	if head != 0 {
		fl.PushTail(head)
	}
	return ptr
}

func (fl *FreeList) PushTail(ptr uint64) {
	//add it to the tail node
	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	//add a new tail node if it's null (the list is never empty)
	if seq2idx(fl.tailSeq) == 0 {
		//try to rescue from the list head
		next, head := flPop(fl) //may remove the head node
		if next == 0 {
			//or allocate a new node by appending
			next = fl.newFreeList(make([]byte, BTREE_PAGE_SIZE))
		}
		//link tyo the new tail node
		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		// also add the head node if it's removed
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

// make the newly added items available for consumption
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

func flPop(fl *FreeList) (ptr uint64, head uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0 //cannot advance
	}

	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq)) //item
	fl.headSeq++
	//move to the next one if the head node is empty
	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
		if fl.headPage == 0 {
			panic("head page cannot be 0")
		}
	}
	return
}
