package core

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	namespace = "gorb" // For Prometheus metrics.
)

var (
	serviceHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "service_health",
		Help:      "Health of the load balancer service",
	}, []string{"service_name", "service_host", "service_port", "protocol"})

	serviceBackends = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "service_backends",
		Help:      "Number of backends in the load balancer service",
	}, []string{"service_name", "service_host", "service_port", "protocol"})

	serviceBackendUptimeTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "service_backend_uptime_seconds",
		Help:      "Uptime in seconds of a backend service",
	}, []string{"service_name", "backend_name", "backend_host", "backend_port"})

	serviceBackendHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "service_backend_health",
		Help:      "Health of a backend service",
	}, []string{"service_name", "backend_name", "backend_host", "backend_port"})

	serviceBackendStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "service_backend_status",
		Help:      "Status of a backend service",
	}, []string{"service_name", "backend_name", "backend_host", "backend_port"})

	serviceBackendWeight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "service_backend_weight",
		Help:      "Weight of a backend service",
	}, []string{"service_name", "backend_name", "backend_host", "backend_port"})
)

type Exporter struct {
	ctx *Context
}

func NewExporter(ctx *Context) *Exporter {
	return &Exporter{
		ctx: ctx,
	}
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	serviceHealth.Describe(ch)
	serviceBackends.Describe(ch)
	serviceBackendUptimeTotal.Describe(ch)
	serviceBackendHealth.Describe(ch)
	serviceBackendStatus.Describe(ch)
	serviceBackendWeight.Describe(ch)
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if err := e.collect(); err != nil {
		log.Errorf("error collecting metrics: %s", err)
		return
	}
	e.sendMetrics(ch)
}

func (e *Exporter) sendMetrics(ch chan<- prometheus.Metric) {
	metrics := []*prometheus.GaugeVec{
		serviceHealth,
		serviceBackends,
		serviceBackendUptimeTotal,
		serviceBackendHealth,
		serviceBackendStatus,
		serviceBackendWeight,
	}
	for _, m := range metrics {
		m.Collect(ch)
		m.Reset()
	}
}

func (e *Exporter) collect() error {
	for serviceName := range e.ctx.services {
		service, err := e.ctx.GetService(serviceName)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error getting service: %s", serviceName))
		}

		serviceHealth.WithLabelValues(serviceName, service.Options.Host, fmt.Sprintf("%d", service.Options.Port),
			service.Options.Protocol).
			Set(service.Health)

		serviceBackends.WithLabelValues(serviceName, service.Options.Host, fmt.Sprintf("%d", service.Options.Port),
			service.Options.Protocol).
			Set(float64(len(service.Backends)))
		for _, backendName := range service.Backends {
			backend, err := e.ctx.GetBackend(serviceName, backendName)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("error getting backend %s from service %s", backendName, serviceName))
			}

			serviceBackendUptimeTotal.WithLabelValues(serviceName, backendName, backend.Options.Host,
				fmt.Sprintf("%d", backend.Options.Port)).
				Set(backend.Metrics.Uptime.Seconds())

			serviceBackendHealth.WithLabelValues(serviceName, backendName, backend.Options.Host,
				fmt.Sprintf("%d", backend.Options.Port)).
				Set(backend.Metrics.Health)

			serviceBackendStatus.WithLabelValues(serviceName, backendName, backend.Options.Host,
				fmt.Sprintf("%d", backend.Options.Port)).
				Set(float64(backend.Metrics.Status))

			serviceBackendWeight.WithLabelValues(serviceName, backendName, backend.Options.Host,
				fmt.Sprintf("%d", backend.Options.Port)).
				Set(float64(backend.Options.weight))
		}
	}
	return nil
}
func RegisterPrometheusExporter(ctx *Context) {
	prometheus.MustRegister(NewExporter(ctx))
}
