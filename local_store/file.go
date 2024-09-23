package local_store

import (
	"errors"
	"github.com/docker/libkv/store"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
)

// Possible local store errors
var (
	emptyRootPath = errors.New("empty root path")
	notAllowed    = errors.New("method is not allowed")
)

type LocalStore struct {
	rootPath string
}

func NewLocalStore(rootPath string) (*LocalStore, error) {
	if rootPath == "" {
		return nil, emptyRootPath
	}
	log.Infof("creating local store by path %s", rootPath)
	return &LocalStore{
		rootPath: rootPath,
	}, nil
}

// ensureDirExist checks path, if not exist - create full path
func (local *LocalStore) ensureDirExist(dirPath string) error {
	var (
		err   error
		exist bool
	)

	exist, err = local.exists(dirPath)
	if err != nil {
		return err
	}
	if !exist {
		err = os.MkdirAll(dirPath, 0755)
	}
	return err
}

// CreateDir create dir by dirPath
func (local *LocalStore) CreateDir(dirPath string) error {
	err := local.ensureDirExist(dirPath)
	return err
}

// Put a value at the specified key
func (local *LocalStore) Put(key string, value []byte, options *store.WriteOptions) error {
	return notAllowed
}

// Get a value given its key
func (local *LocalStore) Get(key string) (*store.KVPair, error) {
	return local.get(key)
}

func (local *LocalStore) get(key string) (*store.KVPair, error) {
	var content []byte
	var kvPair *store.KVPair
	var err error

	content, err = os.ReadFile(key)
	if err != nil {
		return nil, err
	}
	kvPair = &store.KVPair{Key: key, Value: content, LastIndex: 0}
	return kvPair, nil
}

// Delete the value at the specified key
func (local *LocalStore) Delete(key string) error {
	return notAllowed
}

// Verify if a Key exists in the store
func (local *LocalStore) Exists(key string) (bool, error) {
	return local.exists(key)
}

func (local *LocalStore) exists(key string) (bool, error) {
	if _, err := os.Stat(key); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

// Watch for changes on a key
func (local *LocalStore) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, nil
}

// WatchTree watches for changes on child nodes under
// a given directory
func (local *LocalStore) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, nil
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (local *LocalStore) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, nil
}

// List the content of a given prefix
func (local *LocalStore) List(directory string) ([]*store.KVPair, error) {
	return local.list(directory)
}

func (local *LocalStore) list(directory string) ([]*store.KVPair, error) {
	var kvPairs []*store.KVPair
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if !file.IsDir() {
			var kvPair *store.KVPair
			kvPair, err = local.get(path.Join(directory, file.Name()))
			if err != nil {
				return nil, err
			}
			kvPairs = append(kvPairs, kvPair)
		}
	}
	return kvPairs, nil
}

// DeleteTree deletes a range of keys under a given directory
func (local *LocalStore) DeleteTree(directory string) error {
	return notAllowed
}

// Atomic CAS operation on a single value.
// Pass previous = nil to create a new key.
func (local *LocalStore) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	return false, nil, nil
}

// Atomic delete of a single value
func (local *LocalStore) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return false, nil
}

// Close the store connection
func (local *LocalStore) Close() {

}
