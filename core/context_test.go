package core

import (
	"testing"

	"syscall"

	"github.com/qk4l/gorb/disco"
	"github.com/qk4l/gorb/pulse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tehnerd/gnl2go"
)

type fakeDisco struct {
	mock.Mock
}

func (d *fakeDisco) Expose(name, host string, port uint16) error {
	args := d.Called(name, host, port)
	return args.Error(0)
}

func (d *fakeDisco) Remove(name string) error {
	args := d.Called(name)
	return args.Error(0)
}

type fakeIpvs struct {
	mock.Mock
}

func (f *fakeIpvs) Init() error {
	args := f.Called()
	return args.Error(0)
}

func (f *fakeIpvs) Exit() {
	f.Called()
}

func (f *fakeIpvs) Flush() error {
	args := f.Called()
	return args.Error(0)
}

func (f *fakeIpvs) AddService(vip string, port uint16, protocol uint16, sched string) error {
	args := f.Called(vip, port, protocol, sched)
	return args.Error(0)
}

func (f *fakeIpvs) AddServiceWithFlags(vip string, port uint16, protocol uint16, sched string, flags []byte) error {
	args := f.Called(vip, port, protocol, sched, flags)
	return args.Error(0)
}

func (f *fakeIpvs) DelService(vip string, port uint16, protocol uint16) error {
	args := f.Called(vip, port, protocol)
	return args.Error(0)
}

func (f *fakeIpvs) AddDestPort(vip string, vport uint16, rip string, rport uint16, protocol uint16, weight int32, fwd uint32) error {
	args := f.Called(vip, vport, rip, rport, protocol, weight, fwd)
	return args.Error(0)
}

func (f *fakeIpvs) UpdateDestPort(vip string, vport uint16, rip string, rport uint16, protocol uint16, weight int32, fwd uint32) error {
	args := f.Called(vip, vport, rip, rport, protocol, weight, fwd)
	return args.Error(0)

}
func (f *fakeIpvs) DelDestPort(vip string, vport uint16, rip string, rport uint16, protocol uint16) error {
	args := f.Called(vip, vport, rip, rport, protocol)
	return args.Error(0)
}
func (f *fakeIpvs) GetPools() ([]gnl2go.Pool, error) {
	var poolArray []gnl2go.Pool
	return poolArray, nil
}

func newRoutineContext(services map[string]*Service, ipvs Ipvs) *Context {
	c := newContext(ipvs, &fakeDisco{})
	c.services = services
	return c
}

func newContext(ipvs Ipvs, disco disco.Driver) *Context {
	return &Context{
		ipvs:     ipvs,
		services: map[string]*Service{},
		pulseCh:  make(chan pulse.Update),
		stopCh:   make(chan struct{}),
		disco:    disco,
	}
}

var (
	vsID                   = "virtualServiceId"
	rsID                   = "realServerID"
	virtualService         = Service{options: &ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", LbMethod: "sh"}}
	virtualServiceFallBack = Service{options: &ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", Fallback: "fb-zero-to-one"}}
	serviceConfig          = ServiceConfig{
		ServiceOptions:  &ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", LbMethod: "sh", ShFlags: "sh-port|sh-fallback"},
		ServiceBackends: map[string]*BackendOptions{},
	}
)

func TestServiceIsCreated(t *testing.T) {
	options := serviceConfig
	options.ServiceOptions = virtualService.options
	mockIpvs := &fakeIpvs{}
	mockDisco := &fakeDisco{}
	c := newContext(mockIpvs, mockDisco)

	mockIpvs.On("AddService", "127.0.0.1", uint16(80), uint16(syscall.IPPROTO_TCP), "sh").Return(nil)
	mockDisco.On("Expose", vsID, "127.0.0.1", uint16(80)).Return(nil)

	err := c.createService(vsID, &options)
	assert.NoError(t, err)
	mockIpvs.AssertExpectations(t)
	mockDisco.AssertExpectations(t)
}

func TestServiceIsCreatedWithShFlags(t *testing.T) {
	options := serviceConfig
	mockIpvs := &fakeIpvs{}
	mockDisco := &fakeDisco{}
	c := newContext(mockIpvs, mockDisco)

	mockIpvs.On("AddServiceWithFlags", "127.0.0.1", uint16(80), uint16(syscall.IPPROTO_TCP), "sh", gnl2go.U32ToBinFlags(gnl2go.IP_VS_SVC_F_SCHED_SH_FALLBACK|gnl2go.IP_VS_SVC_F_SCHED_SH_PORT)).Return(nil)
	mockDisco.On("Expose", vsID, "127.0.0.1", uint16(80)).Return(nil)

	err := c.createService(vsID, &options)
	assert.NoError(t, err)
	mockIpvs.AssertExpectations(t)
	mockDisco.AssertExpectations(t)
}

func TestPulseUpdateSetsBackendWeightToZeroOnStatusDown(t *testing.T) {
	stash := make(map[pulse.ID]int32)
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{weight: 100}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestPort", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, int32(0), mock.Anything).Return(nil)

	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusDown}})
	assert.Equal(t, len(stash), 1)
	assert.Equal(t, stash[pulse.ID{VsID: vsID, RsID: rsID}], int32(100))
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateSetsBackendWeightWithFallBackZeroToOne(t *testing.T) {
	stash := make(map[pulse.ID]int32)
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{weight: 100}}}
	services := map[string]*Service{vsID: &virtualServiceFallBack}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestPort", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, int32(1), mock.Anything).Return(nil)

	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusDown}})
	assert.Equal(t, len(stash), 1)
	assert.Equal(t, stash[pulse.ID{VsID: vsID, RsID: rsID}], int32(100))
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateIncreasesBackendWeightRelativeToTheHealthOnStatusUp(t *testing.T) {
	stash := map[pulse.ID]int32{pulse.ID{VsID: vsID, RsID: rsID}: int32(12)}
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestPort", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, int32(6), mock.Anything).Return(nil)

	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusUp, Health: 0.5}})
	assert.Equal(t, len(stash), 1)
	assert.Equal(t, stash[pulse.ID{VsID: vsID, RsID: rsID}], int32(12))
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateRemovesStashWhenBackendHasFullyRecovered(t *testing.T) {
	stash := map[pulse.ID]int32{pulse.ID{VsID: vsID, RsID: rsID}: int32(12)}
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestPort", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, int32(12), mock.Anything).Return(nil)

	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusUp, Health: 1}})
	assert.Empty(t, stash)
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateRemovesStashWhenBackendIsDeleted(t *testing.T) {
	stash := map[pulse.ID]int32{pulse.ID{VsID: vsID, RsID: rsID}: int32(0)}
	backends := make(map[string]*Backend)
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)
	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{}})

	assert.Empty(t, stash)
	mockIpvs.AssertExpectations(t)
}

func TestPulseUpdateRemovesStashWhenDeletedAfterNotification(t *testing.T) {
	stash := map[pulse.ID]int32{pulse.ID{VsID: vsID, RsID: rsID}: int32(0)}
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)
	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusRemoved}})

	assert.Empty(t, stash)
	mockIpvs.AssertExpectations(t)
}

func TestStatusDownDuringIncreasingWeight(t *testing.T) {
	stash := map[pulse.ID]int32{pulse.ID{VsID: vsID, RsID: rsID}: int32(100)}
	backends := map[string]*Backend{rsID: &Backend{service: &virtualService, options: &BackendOptions{}}}
	services := map[string]*Service{vsID: &virtualService}
	services[vsID].backends = backends
	mockIpvs := &fakeIpvs{}

	c := newRoutineContext(services, mockIpvs)

	mockIpvs.On("UpdateDestPort", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, int32(0), mock.Anything).Return(nil)
	c.processPulseUpdate(stash, pulse.Update{pulse.ID{VsID: vsID, RsID: rsID}, pulse.Metrics{Status: pulse.StatusDown, Health: 0.5}})

	assert.Equal(t, len(stash), 1)
	assert.Equal(t, stash[pulse.ID{VsID: vsID, RsID: rsID}], int32(100))
	mockIpvs.AssertExpectations(t)
}

func TestServiceIsCreatedWithGenericCustomFlags(t *testing.T) {
	options := &serviceConfig
	options.ServiceOptions.ShFlags = "flag-1|flag-2|flag-3"
	mockIpvs := &fakeIpvs{}
	mockDisco := &fakeDisco{}
	c := newContext(mockIpvs, mockDisco)

	mockIpvs.On("AddServiceWithFlags", "127.0.0.1", uint16(80), uint16(syscall.IPPROTO_TCP), "sh",
		gnl2go.U32ToBinFlags(gnl2go.IP_VS_SVC_F_SCHED1|gnl2go.IP_VS_SVC_F_SCHED2|gnl2go.IP_VS_SVC_F_SCHED3)).Return(nil)
	mockDisco.On("Expose", vsID, "127.0.0.1", uint16(80)).Return(nil)

	err := c.createService(vsID, options)
	assert.NoError(t, err)
	mockIpvs.AssertExpectations(t)
	mockDisco.AssertExpectations(t)
}
