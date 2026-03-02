package btree

import (
	"fmt"
	"os"
	"path"
	"syscall"
)

type KV struct {
	Path string //file name
	fd   int // file descriptor
	tree BTree
}

func (db *KV) Open() error {
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
