package core

import (
	"crypto/tls"
	"errors"
	"github.com/qk4l/gorb/local_store"
	"net/url"
	"path"
	"strings"
	"time"

	"encoding/json"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/boltdb"
	"github.com/docker/libkv/store/consul"
	"github.com/docker/libkv/store/etcd"
	"github.com/docker/libkv/store/zookeeper"
	log "github.com/sirupsen/logrus"
)

// StoreSyncStatus info about synchronization with ext-store
type StoreSyncStatus struct {
	// RemovedServices list of services that can be removed
	RemovedServices []string `json:"removed_services,omitempty"`
	// RemovedBackends list of backends that can be removed
	RemovedBackends []string `json:"removed_backends,omitempty"`
	// UpdatedServices list of services that can be updated
	UpdatedServices []string `json:"updated_services,omitempty"`
	// UpdatedBackends list of backends that can be updated
	UpdatedBackends []string `json:"updated_backends,omitempty"`
	// NewServices list of services that can be added
	NewServices []string `json:"new_services,omitempty"`
	// NewBackends list of backends that can be added
	NewBackends []string `json:"new_backends,omitempty"`
	// Status show final info about sync. May be 'need sync', 'ok'
	Status string `json:"status"`
}

func (sync *StoreSyncStatus) CheckStatus() string {
	if sync.NewServices != nil ||
		sync.NewBackends != nil ||
		sync.UpdatedBackends != nil ||
		sync.UpdatedServices != nil ||
		sync.RemovedBackends != nil ||
		sync.RemovedServices != nil {
		return "need sync"
	} else {
		return "ok"
	}
}

type Store struct {
	ctx              *Context
	kvstore          store.Store
	storeServicePath string
	storeBackendPath string
	stopCh           chan struct{}
}

func NewStore(storeURLs []string, storeServicePath, storeBackendPath string, syncTime int64, useTLS bool, context *Context) (*Store, error) {
	var scheme string
	var storePath string
	var hosts []string
	var kvstore store.Store
	var err error

	for _, storeURL := range storeURLs {
		uri, err := url.Parse(storeURL)
		if err != nil {
			return nil, err
		}
		uriScheme := strings.ToLower(uri.Scheme)
		if scheme != "" && scheme != uriScheme {
			return nil, errors.New("schemes must be the same for all store URLs")
		}
		if storePath != "" && storePath != uri.Path {
			return nil, errors.New("paths must be the same for all store URLs")
		}
		scheme = uriScheme
		storePath = uri.Path
		hosts = append(hosts, uri.Host)
	}

	var usedBackend store.Backend
	switch scheme {
	case "file":
		usedBackend = "file"
	case "consul":
		usedBackend = store.CONSUL
	case "etcd":
		usedBackend = store.ETCD
	case "zookeeper":
		usedBackend = store.ZK
	case "boltdb":
		usedBackend = store.BOLTDB
	case "mock":
		usedBackend = "mock"
	default:
		return nil, errors.New("unsupported uri scheme : " + scheme)
	}
	if usedBackend == "file" {
		kvstore, err = createLocalStore(storePath, storeServicePath, storeBackendPath)
		if err != nil {
			return nil, err
		}
	} else {
		kvstore, err = createExtStore(usedBackend, hosts, useTLS)
		if err != nil {
			return nil, err
		}
	}

	store := &Store{
		ctx:              context,
		kvstore:          kvstore,
		storeServicePath: path.Join(storePath, storeServicePath),
		storeBackendPath: path.Join(storePath, storeBackendPath),
		stopCh:           make(chan struct{}),
	}

	context.SetStore(store)

	store.Sync()
	storeTimer := time.NewTicker(time.Duration(syncTime) * time.Second)
	go func() {
		for {
			select {
			case <-storeTimer.C:
				store.Sync()
			case <-time.After(60 * time.Second):
				log.Error("Timeout 60s was reached for store.Sync()")
			case <-store.stopCh:
				storeTimer.Stop()
				return
			}
		}
	}()

	return store, nil
}

func createLocalStore(storePath string, storeServicePath string, storeBackendPath string) (store.Store, error) {
	kvstore, err := local_store.NewLocalStore(storePath)
	if err != nil {
		return nil, err
	}

	// init store dirs
	if err = kvstore.CreateDir(path.Join(storePath, storeServicePath)); err != nil {
		return nil, err
	}
	if err = kvstore.CreateDir(path.Join(storePath, storeBackendPath)); err != nil {
		return nil, err
	}

	return kvstore, nil
}

func createExtStore(backend store.Backend, hosts []string, useTLS bool) (store.Store, error) {
	storeConfig := &store.Config{
		ConnectionTimeout: 10 * time.Second,
	}

	if useTLS {
		storeConfig.TLS = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	kvstore, err := libkv.NewStore(
		backend,
		hosts,
		storeConfig,
	)
	if err != nil {
		return nil, err
	}
	return kvstore, nil
}

func (s *Store) Sync() {
	services, backends, err := s.getStoreContent()
	if err != nil {
		log.Errorf("error while get data from ext-store: %s", err)
		return
	}
	// synchronize context
	s.ctx.Synchronize(services, backends)
}

func (s *Store) StoreSyncStatus() (*StoreSyncStatus, error) {

	services, backends, err := s.getStoreContent()
	if err != nil {
		return nil, err
	}
	return s.ctx.CompareWithStore(services, backends), nil
}

// StartSyncWithStore synchronize gorb with store
func (s *Store) StartSyncWithStore() error {
	// build external services map
	services, backends, err := s.getStoreContent()
	if err != nil {
		log.Errorf("error while get data from ext-store: %s", err)
		return err
	}

	// synchronize context
	if err = s.ctx.Synchronize(services, backends); err != nil {
		return err
	}
	return nil
}

// getStoreContent extract service and backend from ext-store
func (s *Store) getStoreContent() (map[string]*ServiceOptions, map[string]*BackendOptions, error) {
	// build external services map
	services, err := s.getExternalServices()
	if err != nil {
		log.Errorf("error while get services form ext-store: %s", err)
		return nil, nil, err
	}
	// build external backends map
	backends, err := s.getExternalBackends()
	if err != nil {
		log.Errorf("error while get backends form ext-store: %s", err)
		return nil, nil, err
	}
	return services, backends, nil
}

func (s *Store) getExternalServices() (map[string]*ServiceOptions, error) {
	services := make(map[string]*ServiceOptions)
	// build external service map (temporary all services)
	kvlist, err := s.kvstore.List(s.storeServicePath)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return services, nil
		}
		return nil, err
	}
	for _, kvpair := range kvlist {
		if kvpair.Value == nil {
			continue
		}
		id := s.getID(kvpair.Key)
		var options ServiceOptions
		if err := json.Unmarshal(kvpair.Value, &options); err != nil {
			return nil, err
		}
		services[id] = &options
	}
	return services, nil
}

func (s *Store) getExternalBackends() (map[string]*BackendOptions, error) {
	backends := make(map[string]*BackendOptions)
	// build external backend map
	kvlist, err := s.kvstore.List(s.storeBackendPath)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return backends, nil
		}
		return nil, err
	}
	for _, kvpair := range kvlist {
		if kvpair.Value == nil {
			continue
		}
		var options BackendOptions
		if err := json.Unmarshal(kvpair.Value, &options); err != nil {
			return nil, err
		}
		backends[s.getID(kvpair.Key)] = &options
	}
	return backends, nil
}

func (s *Store) Close() {
	close(s.stopCh)
}

func (s *Store) CreateService(vsID string, opts *ServiceOptions) error {
	// put to store
	if err := s.put(s.storeServicePath+"/"+vsID, opts, false); err != nil {
		log.Errorf("error while put service to store: %s", err)
		return err
	}
	return nil
}

func (s *Store) UpdateService(vsID string, opts *ServiceOptions) error {
	// put to store
	if err := s.put(s.storeServicePath+"/"+vsID, opts, true); err != nil {
		log.Errorf("error while put(update) service to store: %s", err)
		return err
	}
	return nil
}

func (s *Store) CreateBackend(vsID, rsID string, opts *BackendOptions) error {
	opts.VsID = vsID
	// put to store
	if err := s.put(s.storeBackendPath+"/"+rsID, opts, false); err != nil {
		log.Errorf("error while put backend to store: %s", err)
		return err
	}
	return nil
}

func (s *Store) UpdateBackend(vsID, rsID string, opts *BackendOptions) error {
	opts.VsID = vsID
	// put to store
	if err := s.put(s.storeBackendPath+"/"+rsID, opts, true); err != nil {
		log.Errorf("error while put(update) backend to store: %s", err)
		return err
	}
	return nil
}

func (s *Store) RemoveService(vsID string) error {
	if err := s.kvstore.DeleteTree(s.storeServicePath + "/" + vsID); err != nil {
		log.Errorf("error while delete service from store: %s", err)
		return err
	}
	return nil
}

func (s *Store) RemoveBackend(rsID string) error {
	if err := s.kvstore.DeleteTree(s.storeBackendPath + "/" + rsID); err != nil {
		log.Errorf("error while delete backend from store: %s", err)
		return err
	}
	return nil
}

func (s *Store) put(key string, value interface{}, overwrite bool) error {
	// marshal value
	var byteValue []byte
	var isDir bool
	if value == nil {
		byteValue = nil
		isDir = true
	} else {
		_bytes, err := json.Marshal(value)
		if err != nil {
			return err
		}
		byteValue = _bytes
		isDir = false
	}
	// check key exist (create if not exists)
	exist, err := s.kvstore.Exists(key)
	if err != nil {
		return err
	}
	if !exist || overwrite {
		writeOptions := &store.WriteOptions{IsDir: isDir, TTL: 0}
		if err := s.kvstore.Put(key, byteValue, writeOptions); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) getID(key string) string {
	index := strings.LastIndex(key, "/")
	if index <= 0 {
		return key
	}
	return key[index+1:]
}

func init() {
	consul.Register()
	etcd.Register()
	zookeeper.Register()
	boltdb.Register()
}
