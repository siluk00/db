package btree

import "syscall"

type KV struct {
	Path string //file name
	fd   int
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
