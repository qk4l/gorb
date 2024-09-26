package core

import (
	"github.com/qk4l/gorb/pulse"
	log "github.com/sirupsen/logrus"
	"github.com/tehnerd/gnl2go"
)

// Backend RS entity of gorb
type Backend struct {
	rsID    string
	options *BackendOptions
	service *Service
	monitor *pulse.Pulse
	metrics pulse.Metrics
}

// UpdateWeight save new weight and return prev
func (rs *Backend) GetHealth() float64 {
	return rs.metrics.Health
}

// UpdateWeight save new weight and return prev
func (rs *Backend) UpdateWeight(weight int32) int32 {
	var oldValue int32
	oldValue, rs.options.weight = rs.options.weight, weight
	return oldValue
}

// Cleanup backend, gracefully stops monitoring
func (rs *Backend) Cleanup() {
	log.Infof("deregister backend [%s/%s]",
		rs.service.vsID,
		rs.rsID,
	)

	// Stop the pulse goroutine.
	rs.monitor.Stop()

}

// Service VS entity of gorb
type Service struct {
	vsID     string
	options  *ServiceOptions
	svc      gnl2go.Service
	backends map[string]*Backend
}

func (vs *Service) GetBackend(rsID string) (*Backend, bool) {
	rs, ok := vs.backends[rsID]
	return rs, ok
}

func (vs *Service) BackendExist(rsID string) bool {
	if _, ok := vs.backends[rsID]; ok {
		return true
	}
	return false
}

// CreateBackend registers a new backend in the virtual service.
func (vs *Service) CreateBackend(rsID string, opts *BackendOptions) error {
	if err := opts.Validate(); err != nil {
		return err
	}

	log.Infof("register new backend [%s] for virtual service [%s]",
		rsID,
		vs.vsID)

	p, err := pulse.New(opts.host.String(), opts.Port, vs.options.Pulse)
	if err != nil {
		return err
	}
	vs.backends[rsID] = &Backend{options: opts, service: vs, monitor: p}

	return nil
}

// RemoveBackend deregister a backend in the virtual service.
func (vs *Service) RemoveBackend(rsID string) (*BackendOptions, error) {
	rs, exists := vs.backends[rsID]
	if !exists {
		return nil, ErrObjectNotFound
	}
	rs.Cleanup()
	// deregister
	delete(rs.service.backends, rsID)
	return rs.options, nil
}

// Cleanup remove service backends, gracefully stops backend monitoring
func (vs *Service) Cleanup() {
	for rsID, backend := range vs.backends {
		log.Infof("cleaning up now orphaned backend [%s/%s]", vs.vsID, rsID)

		// Stop the pulse goroutine.
		backend.monitor.Stop()

		delete(vs.backends, rsID)
	}
}

func (vs *Service) CalcServiceStat() *ServiceInfo {
	status := &ServiceInfo{
		Options:       vs.options,
		Backends:      make([]string, 0, len(vs.backends)),
		BackendsCount: uint16(len(vs.backends)),
		FallBack:      vs.options.Fallback,
	}

	if status.BackendsCount != 0 {
		// Calculate backends health
		for rsKey, rs := range vs.backends {
			status.Health += rs.GetHealth()
			status.Backends = append(status.Backends, rsKey)
		}
		status.Health /= float64(status.BackendsCount)
	} else {
		// Service without backends could not be healthy
		status.Health = 0.0
	}
	return status
}
