package core

import (
	"crypto/tls"
	"errors"
	"github.com/qk4l/gorb/local_store"
	"gopkg.in/yaml.v3"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/boltdb"
	"github.com/docker/libkv/store/consul"
	"github.com/docker/libkv/store/etcd"
	"github.com/docker/libkv/store/zookeeper"
	log "github.com/sirupsen/logrus"
)

type ServiceConfig struct {
	ServiceOptions  *ServiceOptions            `yaml:"service_options"`
	ServiceBackends map[string]*BackendOptions `yaml:"service_backends"`
}

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

	var storeBackend store.Backend
	switch scheme {
	case "file":
		storeBackend = "file"
	case "consul":
		storeBackend = store.CONSUL
	case "etcd":
		storeBackend = store.ETCD
	case "zookeeper":
		storeBackend = store.ZK
	case "boltdb":
		storeBackend = store.BOLTDB
	case "mock":
		storeBackend = "mock"
	default:
		return nil, errors.New("unsupported uri scheme : " + scheme)
	}
	if storeBackend == "file" {
		kvstore, err = createLocalStore(storePath, storeServicePath, storeBackendPath)
		if err != nil {
			return nil, err
		}
	} else {
		kvstore, err = createExtStore(storeBackend, hosts, useTLS)
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
	if syncTime > 0 {
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
	}
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
	services, err := s.getStoreServices()
	if err != nil {
		log.Errorf("error while get data from ext-store: %s", err)
		return
	}
	// synchronize context
	s.ctx.Synchronize(services)
}

func (s *Store) StoreSyncStatus() (*StoreSyncStatus, error) {

	services, err := s.getStoreServices()
	if err != nil {
		return nil, err
	}
	return s.ctx.CompareWith(services), nil
}

// StartSyncWithStore synchronize gorb with store
func (s *Store) StartSyncWithStore() error {
	// build external services map
	services, err := s.getStoreServices()
	if err != nil {
		log.Errorf("error while get data from ext-store: %s", err)
		return err
	}

	// synchronize context
	if err = s.ctx.Synchronize(services); err != nil {
		return err
	}
	return nil
}

func (s *Store) getStoreServices() (map[string]*ServiceConfig, error) {
	services := make(map[string]*ServiceConfig)
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
		var options ServiceConfig
		if err := yaml.Unmarshal(kvpair.Value, &options); err != nil {
			return nil, err
		}
		if options.ServiceOptions == nil {
			continue
		} else {
			options.ServiceOptions.Validate(nil)
		}
		services[id] = &options
	}
	return services, nil
}

func (s *Store) Close() {
	close(s.stopCh)
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
