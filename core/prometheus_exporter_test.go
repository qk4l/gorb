package core

import (
	"testing"

	"github.com/qk4l/gorb/pulse"
)

var (
	service = &Service{options: &ServiceOptions{
		Host:       "localhost",
		Port:       1234,
		Protocol:   "tcp",
		LbMethod:   "wlc",
		Persistent: true,
	}}
	backend = &Backend{
		options: &BackendOptions{
			Host:   "localhost",
			Port:   1234,
			weight: 1,
			vsID:   "service1",
		},
		monitor: &pulse.Pulse{}}
)

func TestCollector(t *testing.T) {
	service.backends = map[string]*Backend{"service1-backend1": backend}
	ctx := &Context{
		services: map[string]*Service{"service1": service},
	}

	exporter := NewExporter(ctx)
	err := exporter.collect()
	if err != nil {
		t.Fatal(err)
	}
}
