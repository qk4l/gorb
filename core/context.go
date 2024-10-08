/*
   Copyright (c) 2015 Andrey Sibiryov <me@kobology.ru>
   Copyright (c) 2015 Other contributors as noted in the AUTHORS file.

   This file is part of GORB - Go Routing and Balancing.

   GORB is free software; you can redistribute it and/or modify
   it under the terms of the GNU Lesser General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   GORB is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public License
   along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package core

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/qk4l/gorb/disco"
	"github.com/qk4l/gorb/pulse"
	"github.com/qk4l/gorb/util"
	"github.com/vishvananda/netlink"

	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/tehnerd/gnl2go"
)

// Possible runtime errors.
var (
	schedulerFlags = map[string]int{
		"sh-fallback": gnl2go.IP_VS_SVC_F_SCHED_SH_FALLBACK,
		"sh-port":     gnl2go.IP_VS_SVC_F_SCHED_SH_PORT,
		"flag-1":      gnl2go.IP_VS_SVC_F_SCHED1,
		"flag-2":      gnl2go.IP_VS_SVC_F_SCHED2,
		"flag-3":      gnl2go.IP_VS_SVC_F_SCHED3,
	}
	fallbackFlags = map[string]int16{
		"fb-default":     Default,
		"fb-zero-to-one": ZeroToOne,
	}
	ErrIpvsSyscallFailed = errors.New("error while calling into IPVS")
	ErrObjectExists      = errors.New("specified object already exists")
	ErrObjectNotFound    = errors.New("unable to locate specified object")
	ErrIncompatibleAFs   = errors.New("incompatible address families")
)

// Fallback options
const (
	// Default - Set 0 weight to failed backend
	Default int16 = iota
	// ZeroToOne - Set weight 1 to all if all backends have StatusDown
	ZeroToOne
)

// Context abstacts away the underlying IPVS bindings implementation.
type Context struct {
	ipvs         Ipvs
	endpoint     net.IP
	services     map[string]*Service
	mutex        sync.RWMutex
	pulseCh      chan pulse.Update
	disco        disco.Driver
	stopCh       chan struct{}
	vipInterface netlink.Link
	store        *Store
}

type Ipvs interface {
	Init() error
	Exit()
	Flush() error
	AddService(vip string, port uint16, protocol uint16, sched string) error
	AddServiceWithFlags(vip string, port uint16, protocol uint16, sched string, flags []byte) error
	DelService(vip string, port uint16, protocol uint16) error
	AddDestPort(vip string, vport uint16, rip string, rport uint16, protocol uint16, weight int32, fwd uint32) error
	UpdateDestPort(vip string, vport uint16, rip string, rport uint16, protocol uint16, weight int32, fwd uint32) error
	DelDestPort(vip string, vport uint16, rip string, rport uint16, protocol uint16) error
	// Unforture not work =(
	// GetPoolForService(svc gnl2go.Service) (gnl2go.Pool, error)
	GetPools() ([]gnl2go.Pool, error)
}

// NewContext creates a new Context and initializes IPVS.
func NewContext(options ContextOptions) (*Context, error) {
	log.Info("initializing IPVS context")

	ctx := &Context{
		ipvs:     &gnl2go.IpvsClient{},
		services: make(map[string]*Service),
		pulseCh:  make(chan pulse.Update),
		stopCh:   make(chan struct{}),
	}

	if len(options.Disco) > 0 {
		log.Infof("creating Consul client with Agent URL: %s", options.Disco)

		var err error

		ctx.disco, err = disco.New(&disco.Options{
			Type: "consul",
			Args: util.DynamicMap{"URL": options.Disco}})

		if err != nil {
			return nil, err
		}
	} else {
		ctx.disco, _ = disco.New(&disco.Options{Type: "none"})
	}

	if len(options.Endpoints) > 0 {
		// TODO(@kobolog): Bind virtual services on multiple endpoints.
		ctx.endpoint = options.Endpoints[0]
		if options.ListenPort != 0 {
			log.Info("Registered the REST service to Consul.")
			ctx.disco.Expose("gorb", ctx.endpoint.String(), options.ListenPort)
		}
	}

	if err := ctx.ipvs.Init(); err != nil {
		log.Errorf("unable to initialize IPVS context: %s", err)

		// Here and in other places: IPVS errors are abstracted to make GNL2GO
		// replaceable in the future, since it's not really maintained anymore.
		return nil, ErrIpvsSyscallFailed
	}

	if options.Flush && ctx.ipvs.Flush() != nil {
		log.Errorf("unable to clean up IPVS pools - ensure ip_vs is loaded")
		ctx.Close()
		return nil, ErrIpvsSyscallFailed
	}

	if options.VipInterface != "" {
		var err error
		if ctx.vipInterface, err = netlink.LinkByName(options.VipInterface); err != nil {
			ctx.Close()
			return nil, fmt.Errorf(
				"unable to find the interface '%s' for VIPs: %s",
				options.VipInterface, err)
		}
		log.Infof("VIPs will be added to interface '%s'", ctx.vipInterface.Attrs().Name)
	}

	// Fire off a pulse notifications sink goroutine.
	go ctx.run()

	return ctx, nil
}

// Close shuts down IPVS and closes the Context.
func (ctx *Context) Close() {
	log.Info("shutting down IPVS context")

	// This will also shutdown the pulse notification sink goroutine.
	close(ctx.stopCh)

	for vsID := range ctx.services {
		ctx.RemoveService(vsID)
	}

	// This is not strictly required, as far as I know.
	ctx.ipvs.Exit()
}

// ipvs.GetPoolForService() not works =( impement via iteration
func (ctx *Context) GetPoolForService(svc gnl2go.Service) (gnl2go.Pool, error) {
	ipvs_pools, err := ctx.ipvs.GetPools()
	if err != nil {
		log.Errorf("Failed to get pools from ipvs: %s", err)
		return gnl2go.Pool{}, ErrIpvsSyscallFailed
	}

	log.Debugf("IPVS has %d polls", len(ipvs_pools))

	for _, ipvs_pool := range ipvs_pools {
		if ipvs_pool.Service.IsEqual(svc) {
			return ipvs_pool, nil
		}
	}
	return gnl2go.Pool{}, fmt.Errorf("service doesn't exist\n")
}

// CreateService registers a new virtual service with IPVS.
func (ctx *Context) createService(vsID string, serviceConfig *ServiceConfig) error {
	serviceOptions := serviceConfig.ServiceOptions
	if err := serviceOptions.Validate(ctx.endpoint); err != nil {
		return err
	}

	if _, exists := ctx.services[vsID]; exists {
		return ErrObjectExists
	}

	if ctx.vipInterface != nil {
		ifName := ctx.vipInterface.Attrs().Name
		vip := &netlink.Addr{IPNet: &net.IPNet{
			IP: net.ParseIP(serviceOptions.host.String()), Mask: net.IPv4Mask(255, 255, 255, 255)}}
		if err := netlink.AddrAdd(ctx.vipInterface, vip); err != nil {
			log.Infof(
				"failed to add VIP %s to interface '%s' for service [%s]: %s",
				serviceOptions.host, ifName, vsID, err)
		} else {
			serviceOptions.delIfAddr = true
		}
		log.Infof("VIP %s has been added to interface '%s'", serviceOptions.host, ifName)
	}

	log.Infof("creating virtual service [%s] on %s:%d", vsID, serviceOptions.host,
		serviceOptions.Port)

	var svc = gnl2go.Service{
		Proto: serviceOptions.protocol,
		VIP:   serviceOptions.host.String(),
		Port:  serviceOptions.Port,
		Sched: serviceOptions.LbMethod,
	}

	var flags int
	for _, flag := range strings.Split(serviceOptions.ShFlags, "|") {
		flags = flags | schedulerFlags[flag]
		if flags != 0 {
			svc.Flags = gnl2go.U32ToBinFlags(uint32(flags))
		}
	}

	_, err := ctx.GetPoolForService(svc)

	if err == nil {
		log.Infof("Service %s:%d already existed skip creation", svc.VIP, svc.Port)
	} else {
		if flags != 0 {
			if err := ctx.ipvs.AddServiceWithFlags(
				svc.VIP,
				svc.Port,
				svc.Proto,
				svc.Sched,
				svc.Flags,
			); err != nil {
				log.Errorf("error while creating virtual service: %s", err)
				return ErrIpvsSyscallFailed
			}
		} else {
			if err := ctx.ipvs.AddService(
				svc.VIP,
				svc.Port,
				svc.Proto,
				svc.Sched,
			); err != nil {
				log.Errorf("error while creating virtual service: %s", err)
				return ErrIpvsSyscallFailed
			}
		}
	}

	ctx.services[vsID] = &Service{vsID: vsID, options: serviceOptions, svc: svc, backends: make(map[string]*Backend)}

	if err := ctx.disco.Expose(vsID, serviceOptions.host.String(), serviceOptions.Port); err != nil {
		log.Errorf("error while exposing service to Disco: %s", err)
	}

	// init backends
	for rsID, backendOpts := range serviceConfig.ServiceBackends {
		err := ctx.createBackend(vsID, rsID, backendOpts)
		if err != nil {
			return err
		}
	}

	return nil
}

// CreateService registers a new virtual service with IPVS.
func (ctx *Context) CreateService(vsID string, serviceConfig *ServiceConfig) error {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.createService(vsID, serviceConfig)
}

// CreateBackend registers a new backend with a virtual service.
func (ctx *Context) createBackend(vsID, rsID string, opts *BackendOptions) error {
	var skipCreation bool

	// Validate input
	vs, exists := ctx.services[vsID]
	if !exists {
		return fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}
	if vs.BackendExist(rsID) {
		return fmt.Errorf("%w rsID: %s", ErrObjectExists, rsID)
	}
	if err := opts.Validate(); err != nil {
		return err
	}

	if util.AddrFamily(opts.host) != util.AddrFamily(vs.options.host) {
		return ErrIncompatibleAFs
	}

	log.Infof("creating backend [%s] on %s:%d for virtual service [%s]",
		rsID,
		opts.host,
		opts.Port,
		vsID)

	var newDest = gnl2go.Dest{
		IP:     opts.host.String(),
		Weight: vs.options.MaxWeight,
		Port:   opts.Port,
	}

	pool, err := ctx.GetPoolForService(vs.svc)
	if err != nil {
		log.Errorf("Failed to get pool for service [%s]: %s", vs.svc.VIP, err)
		return ErrIpvsSyscallFailed
	}

	for _, dest := range pool.Dests {
		if dest.IP == newDest.IP && dest.Port == newDest.Port {
			log.Infof("Backend %s:%d already existed in service [%s]. Skip creation", newDest.IP, newDest.Port, vsID)
			skipCreation = true
		}
	}

	if skipCreation == false {
		if err := ctx.ipvs.AddDestPort(
			vs.options.host.String(),
			vs.options.Port,
			newDest.IP,
			newDest.Port,
			vs.options.protocol,
			newDest.Weight,
			vs.options.methodID,
		); err != nil {
			log.Errorf("error while creating backend [%s/%s]: %s", vsID, rsID, err)
			return ErrIpvsSyscallFailed
		}
	}

	err = vs.CreateBackend(rsID, opts)
	if err != nil {
		return err
	}

	// Fire off the configured pulse goroutine, attach it to the Context.
	go vs.backends[rsID].monitor.Loop(pulse.ID{VsID: vsID, RsID: rsID}, ctx.pulseCh, ctx.stopCh)

	return nil
}

// CreateBackend registers a new backend with a virtual service.
func (ctx *Context) CreateBackend(vsID, rsID string, opts *BackendOptions) error {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.createBackend(vsID, rsID, opts)
}

// UpdateBackend updates the specified backend's weight.
func (ctx *Context) updateBackend(vsID, rsID string, weight int32) (int32, error) {

	vs, exists := ctx.services[vsID]
	if !exists {
		return 0, fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}
	rs, exists := vs.backends[rsID]
	if !exists {
		return 0, ErrObjectNotFound
	}

	log.Infof("updating backend [%s/%s] with weight: %d", vsID, rsID,
		weight)

	if err := ctx.ipvs.UpdateDestPort(
		rs.service.options.host.String(),
		rs.service.options.Port,
		rs.options.host.String(),
		rs.options.Port,
		rs.service.options.protocol,
		weight,
		vs.options.methodID,
	); err != nil {
		log.Errorf("error while updating backend [%s/%s]", vsID, rsID)
		return 0, ErrIpvsSyscallFailed
	}

	// Save the old backend weight and update the current backend weight.
	prevWeight := rs.UpdateWeight(weight)

	// Currently the backend options are changing only the weight.
	// The weight value is set to the value requested at the first setting,
	// and the weight value is updated when the pulse fails in the gorb.
	// In kvstore, it seems correct to record the request at the first setting and
	// not reflect the updated weight value.
	//if ctx.store != nil {
	//	ctx.store.UpdateBackend(vsID, rsID, rs.options)
	//}

	return prevWeight, nil
}

// UpdateBackend updates the specified backend's weight.
func (ctx *Context) UpdateBackend(vsID, rsID string, weight int32) (int32, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.updateBackend(vsID, rsID, weight)
}

// RemoveService deregisters a virtual service.
func (ctx *Context) removeService(vsID string) (*ServiceOptions, error) {
	vs, exists := ctx.services[vsID]
	if !exists {
		return nil, fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}

	if ctx.vipInterface != nil && vs.options.delIfAddr == true {
		ifName := ctx.vipInterface.Attrs().Name
		vip := &netlink.Addr{IPNet: &net.IPNet{
			IP: net.ParseIP(vs.options.host.String()), Mask: net.IPv4Mask(255, 255, 255, 255)}}
		if err := netlink.AddrDel(ctx.vipInterface, vip); err != nil {
			log.Infof(
				"failed to delete VIP %s to interface '%s' for service [%s]: %s",
				vs.options.host, ifName, vsID, err)
		}
		log.Infof("VIP %s has been deleted from interface '%s'", vs.options.host, ifName)
	}

	log.Infof("removing virtual service [%s] from %s:%d", vsID,
		vs.options.host,
		vs.options.Port)

	if err := ctx.ipvs.DelService(
		vs.options.host.String(),
		vs.options.Port,
		vs.options.protocol,
	); err != nil {
		log.Errorf("error while removing virtual service [%s] from ipvs: %s", vsID, err)
		return nil, ErrIpvsSyscallFailed
	}

	delete(ctx.services, vsID)
	vs.Cleanup()

	// TODO(@kobolog): This will never happen in case of gorb-link.
	if err := ctx.disco.Remove(vsID); err != nil {
		log.Errorf("error while removing service from Disco: %s", err)
	}

	return vs.options, nil
}

// RemoveService deregisters a virtual service.
func (ctx *Context) RemoveService(vsID string) (*ServiceOptions, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.removeService(vsID)
}

// RemoveBackend deregisters a backend.
func (ctx *Context) removeBackend(vsID, rsID string) (*BackendOptions, error) {
	vs, exist := ctx.services[vsID]
	if !exist {
		return nil, fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}
	rs, exists := vs.backends[rsID]
	if !exists {
		return nil, ErrObjectNotFound
	}

	log.Infof("removing backend [%s/%s]", vsID, rsID)

	if err := ctx.ipvs.DelDestPort(
		vs.options.host.String(),
		vs.options.Port,
		rs.options.host.String(),
		rs.options.Port,
		rs.service.options.protocol,
	); err != nil {
		log.Errorf("error while removing backend [%s/%s] form ipvs: %s", vsID, rsID, err)
		return nil, ErrIpvsSyscallFailed
	}

	return vs.RemoveBackend(rsID)
}

// RemoveBackend deregisters a backend.
func (ctx *Context) RemoveBackend(vsID, rsID string) (*BackendOptions, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.removeBackend(vsID, rsID)
}

// ListServices returns a list of all registered services.
func (ctx *Context) ListServices() ([]string, error) {
	ctx.mutex.RLock()
	defer ctx.mutex.RUnlock()

	r := make([]string, 0, len(ctx.services))

	for vsID := range ctx.services {
		r = append(r, vsID)
	}

	return r, nil
}

// ServiceInfo contains information about virtual service options,
// its backends and overall virtual service health.
type ServiceInfo struct {
	Options       *ServiceOptions `json:"options"`
	Health        float64         `json:"health"`
	Backends      []string        `json:"backends"`
	BackendsCount uint16          `json:"backends_count"`
	FallBack      string          `json:"fallback"`
}

// GetService returns information about a virtual service.
func (ctx *Context) GetService(vsID string) (*ServiceInfo, error) {
	ctx.mutex.RLock()
	defer ctx.mutex.RUnlock()

	vs, exists := ctx.services[vsID]

	if !exists {
		return nil, ErrObjectNotFound
	}
	serviceStats := vs.CalcServiceStat()

	return serviceStats, nil
}

// BackendInfo contains information about backend options and pulse.
type BackendInfo struct {
	Options *BackendOptions `json:"options"`
	Metrics pulse.Metrics   `json:"metrics"`
}

// GetBackend returns information about a backend.
func (ctx *Context) GetBackend(vsID, rsID string) (*BackendInfo, error) {
	ctx.mutex.RLock()
	defer ctx.mutex.RUnlock()

	vs, exists := ctx.services[vsID]
	if !exists {
		return nil, fmt.Errorf("%w vsID: %s", ErrObjectNotFound, vsID)
	}

	rs, exists := vs.backends[rsID]
	if !exists {
		return nil, fmt.Errorf("%w rsID: %s", ErrObjectNotFound, rsID)
	}

	return &BackendInfo{rs.options, rs.metrics}, nil
}

// SetStore if external kvstore exists, set store to context
func (ctx *Context) SetStore(store *Store) {
	ctx.store = store
}

// StoreExist Checks if store set
func (ctx *Context) StoreExist() bool {
	if ctx.store == nil {
		return false
	}
	return true
}

func (ctx *Context) CompareWith(storeServices map[string]*ServiceConfig) *StoreSyncStatus {
	ctx.mutex.RLock()
	defer ctx.mutex.RUnlock()
	syncStatus := &StoreSyncStatus{}

	// find removed services in store
	for vsID, service := range ctx.services {
		if storeServiceOptions, ok := storeServices[vsID]; !ok {
			log.Debugf("service [%s] not found in store", vsID)
			syncStatus.RemovedServices = append(syncStatus.RemovedServices, vsID)
		} else {
			// find updated services in store
			if !service.options.CompareStoreOptions(storeServiceOptions.ServiceOptions) {
				log.Debugf("service [%s] is outdated.", vsID)
				syncStatus.UpdatedServices = append(syncStatus.UpdatedServices, vsID)
			}
			for rsID, backend := range service.backends {
				backendName := fmt.Sprintf("[%s/%s]", vsID, rsID)
				if storeBackendOptions, ok := storeServiceOptions.ServiceBackends[rsID]; !ok {
					log.Debugf("backend %s not found in store", backendName)
					syncStatus.RemovedBackends = append(syncStatus.RemovedBackends, backendName)
				} else {
					// find updated backends
					if !backend.options.CompareStoreOptions(storeBackendOptions) {
						log.Debugf("backend %s is outdated.", backendName)
						syncStatus.UpdatedBackends = append(syncStatus.UpdatedBackends, backendName)
					}
					delete(storeServiceOptions.ServiceBackends, rsID)
				}
			}
			// find new Backends
			for rsID, storeBackend := range storeServiceOptions.ServiceBackends {
				backendName := fmt.Sprintf("[%s/%s]", storeBackend.vsID, rsID)
				log.Debugf("new backend %s found.", backendName)
				syncStatus.NewBackends = append(syncStatus.NewBackends, backendName)
			}
			delete(storeServices, vsID)
		}
	}

	// find new services
	for id, _ := range storeServices {
		log.Debugf("new service [%s] found.", id)
		syncStatus.NewServices = append(syncStatus.NewServices, id)
	}

	syncStatus.Status = syncStatus.CheckStatus()
	return syncStatus
}

func (ctx *Context) Synchronize(storeServicesConfig map[string]*ServiceConfig) error {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	defer log.Info("============================ END SYNC ============================")
	log.Info("============================== SYNC ==============================")

	log.Debug("external store content")
	for vsID, service := range storeServicesConfig {
		log.Debugf("SERVICE[%s]: %#v", vsID, service)
	}

	log.Info("sync services")
	// synchronize services with store
	for vsID, service := range ctx.services {
		if storeService, ok := storeServicesConfig[vsID]; !ok {
			log.Debugf("service [%s] not found. removing", vsID)
			if _, err := ctx.removeService(vsID); err != nil {
				return err
			}
		} else {
			if !service.options.CompareStoreOptions(storeService.ServiceOptions) {
				if _, err := ctx.removeService(vsID); err != nil {
					return err
				}
				if err := ctx.createService(vsID, storeService); err != nil {
					return err
				}
			}
			for rsID, backend := range service.backends {
				if storeBackendOptions, ok := storeService.ServiceBackends[rsID]; !ok {
					log.Debugf("backend [%s/%s] not found in store", vsID, rsID)
					if _, err := ctx.removeBackend(vsID, rsID); err != nil {
						return err
					}
				} else {
					// find updated backends
					if !backend.options.CompareStoreOptions(storeBackendOptions) {
						log.Debugf("backend [%s/%s] is outdated.", vsID, rsID)
						if _, err := ctx.removeBackend(vsID, rsID); err != nil {
							return err
						}
						if err := ctx.createBackend(vsID, rsID, storeBackendOptions); err != nil {
							return err
						}

					}
					delete(storeService.ServiceBackends, rsID)
				}
			}
			log.Infof("create new backends for [%s]. count: %d", vsID, len(storeService.ServiceBackends))
			for rsID, storeBackendOptions := range storeService.ServiceBackends {
				if err := ctx.createBackend(vsID, rsID, storeBackendOptions); err != nil {
					return err
				}
			}
			delete(storeServicesConfig, vsID)
		}
	}
	log.Infof("create new services. count: %d", len(storeServicesConfig))
	for id, storeServiceOptions := range storeServicesConfig {
		if err := ctx.createService(id, storeServiceOptions); err != nil {
			return err
		}
	}

	log.Info("Successfully synced with store")
	return nil
}
