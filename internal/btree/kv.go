package btree

import (
	"fmt"
	"os"
	"path"
	"syscall"
)

type KV struct {
	Path string //file name
	fd   int    // file descriptor
	tree BTree
	mmap struct {
		total  int
		chunks [][]byte
	}
	page struct {
		flushed uint64
		temp    [][]byte
	}
}

func (db *KV) Open() error {
	db.tree.get = db.pageRead
	db.tree.newBNode = db.pageAppend
	db.tree.del = func(uint64) {}
	return nil
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return updateFile(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, updateFile(db)
}

func updateFile(db *KV) error {
	// Write new nodes
	if err := writePages(db); err != nil {
		return err
	}

	// Force ordering
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}

	// update root pointer atomically
	if err := updateRoot(db); err != nil {
		return err
	}

	// make everything persistent
	return syscall.Fsync(db.fd)
}

func writePages(db *KV) error {
	return nil
}

func updateRoot(db *KV) error {
	return nil
}

func createFileSync(file string) (int, error) {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)

	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}

	if err = syscall.Fsync(dirfd); err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}

	return fd, nil
}

func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)

	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}

	panic("bad pointer")
}

func extendMap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil
	}

	alloc := max(db.mmap.total, 64<<20)
	for db.mmap.total+alloc < size {
		alloc *= 2
	}

	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func (db *KV) pageAppend(node []byte) (uint64, error) {
	return uint64(0), nil
}
