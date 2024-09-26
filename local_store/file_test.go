package local_store

import (
	"github.com/docker/libkv/store"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

var (
	dirPath   = "/tmp/gorb_tests/ensureDirExist/test"
	content1  = "test1"
	content2  = "test2"
	fileName1 = "testFile1"
	fileName2 = "testFile2"
)

func TestLocalStore_ensureDirExist(t *testing.T) {
	assert := assert.New(t)
	defer os.RemoveAll("/tmp/gorb_tests")
	fstore := LocalStore{rootPath: "/tmp/gorb_tests"}
	exist, err := fstore.exists(dirPath)
	assert.NoError(err)
	assert.False(exist)
	err = fstore.ensureDirExist(dirPath)
	assert.NoError(err)
	exist, err = fstore.exists(dirPath)
	assert.True(exist)
}

func TestLocalStore_put_createAndUpdate(t *testing.T) {
	defer os.RemoveAll("/tmp/gorb_tests")

	var exist bool
	var content []byte

	assert := assert.New(t)
	filePath := path.Join(dirPath, fileName1)
	fstore := LocalStore{rootPath: "/tmp/gorb_tests"}

	err := fstore.ensureDirExist(dirPath)
	assert.NoError(err)

	err = fstore.put(filePath, []byte(content1), &store.WriteOptions{})
	assert.NoError(err)

	exist, err = fstore.exists(filePath)
	assert.True(exist)

	content, err = os.ReadFile(filePath)
	assert.Equal([]byte(content1), content)

	err = fstore.put(filePath, []byte(content2), &store.WriteOptions{})
	assert.NoError(err)

	content, err = os.ReadFile(filePath)
	assert.Equal([]byte(content2), content)
}

func TestLocalStore_get(t *testing.T) {
	defer os.RemoveAll("/tmp/gorb_tests")

	var exist bool
	var kvPair *store.KVPair

	assert := assert.New(t)
	filePath := path.Join(dirPath, fileName1)
	fstore := LocalStore{rootPath: "/tmp/gorb_tests"}
	expectedKvPair := &store.KVPair{Key: filePath, Value: []byte(content1), LastIndex: 0}

	err := fstore.ensureDirExist(dirPath)
	assert.NoError(err)

	err = fstore.put(filePath, []byte(content1), &store.WriteOptions{})
	assert.NoError(err)

	exist, err = fstore.exists(filePath)
	assert.True(exist)

	kvPair, err = fstore.get(filePath)
	assert.Equal(expectedKvPair, kvPair)
}

func TestLocalStore_exist(t *testing.T) {
	defer os.RemoveAll("/tmp/gorb_tests")

	var exist bool

	assert := assert.New(t)
	filePath := path.Join(dirPath, fileName1)
	fstore := LocalStore{rootPath: "/tmp/gorb_tests"}

	err := fstore.ensureDirExist(dirPath)
	assert.NoError(err)

	exist, err = fstore.exists(filePath)
	assert.NoError(err)
	assert.False(exist)

	err = fstore.put(filePath, []byte{}, &store.WriteOptions{})
	assert.NoError(err)

	exist, err = fstore.exists(filePath)
	assert.NoError(err)
	assert.True(exist)
}

func TestLocalStore_list(t *testing.T) {
	defer os.RemoveAll("/tmp/gorb_tests")

	assert := assert.New(t)
	filePath1 := path.Join(dirPath, fileName1)
	filePath2 := path.Join(dirPath, fileName2)
	kvPair1 := &store.KVPair{Key: filePath1, Value: []byte(content1), LastIndex: 0}
	kvPair2 := &store.KVPair{Key: filePath2, Value: []byte(content2), LastIndex: 0}
	fstore := LocalStore{rootPath: "/tmp/gorb_tests"}

	err := fstore.ensureDirExist(dirPath)
	assert.NoError(err)

	err = fstore.put(filePath1, []byte(content1), &store.WriteOptions{})
	assert.NoError(err)

	err = fstore.put(filePath2, []byte(content2), &store.WriteOptions{})
	assert.NoError(err)
	var kvPairs []*store.KVPair
	kvPairs, err = fstore.list(dirPath)
	assert.NoError(err)
	assert.Equal([]*store.KVPair{kvPair1, kvPair2}, kvPairs)
}
