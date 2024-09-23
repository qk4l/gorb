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
	"net"
	"strings"
	"syscall"

	"github.com/qk4l/gorb/pulse"

	"github.com/tehnerd/gnl2go"
)

// Possible validation errors.
var (
	ErrMissingEndpoint     = errors.New("endpoint information is missing")
	ErrUnknownMethod       = errors.New("specified forwarding method is unknown")
	ErrUnknownProtocol     = errors.New("specified protocol is unknown")
	ErrUnknownFlag         = errors.New("specified flag is unknown")
	ErrUnknownFallbackFlag = errors.New("specified fallback flag is unknown")
)

// ContextOptions configure Context behavior.
type ContextOptions struct {
	Disco        string
	Endpoints    []net.IP
	Flush        bool
	ListenPort   uint16
	VipInterface string
}

// ServiceOptions describe a virtual service.
type ServiceOptions struct {
	//service settings
	Host       string `json:"host" yaml:"host"`
	Port       uint16 `json:"port" yaml:"port"`
	Protocol   string `json:"protocol" yaml:"protocol"`
	LbMethod   string `json:"lb_method" yaml:"lb_method"`
	ShFlags    string `json:"sh_flags" yaml:"sh_flags"`
	Persistent bool   `json:"persistent" yaml:"persistent"`
	Fallback   string `json:"fallback" yaml:"fallback"`

	// service backends settings
	FwdMethod string         `json:"fwd_method" yaml:"fwd_method"`
	Pulse     *pulse.Options `json:"pulse" yaml:"pulse"`
	MaxWeight int32          `json:"max_weight" yaml:"max_weight"`

	// Host string resolved to an IP, including DNS lookup.
	host      net.IP
	delIfAddr bool

	// Protocol string converted to a protocol number.
	protocol uint16

	// Forwarding method string converted to a forwarding method number.
	methodID uint32
}

// Validate fills missing fields and validates virtual service configuration.
func (o *ServiceOptions) Validate(defaultHost net.IP) error {
	if o.Port == 0 {
		return ErrMissingEndpoint
	}

	if len(o.Host) != 0 {
		if addr, err := net.ResolveIPAddr("ip", o.Host); err == nil {
			o.host = addr.IP
		} else {
			return err
		}
	} else if defaultHost != nil {
		o.host = defaultHost
	} else {
		return ErrMissingEndpoint
	}

	if len(o.Protocol) == 0 {
		o.Protocol = "tcp"
	}

	o.Protocol = strings.ToLower(o.Protocol)

	switch o.Protocol {
	case "tcp":
		o.protocol = syscall.IPPROTO_TCP
	case "udp":
		o.protocol = syscall.IPPROTO_UDP
	default:
		return ErrUnknownProtocol
	}

	if o.ShFlags != "" {
		for _, flag := range strings.Split(o.ShFlags, "|") {
			if _, ok := schedulerFlags[flag]; !ok {
				return ErrUnknownFlag
			}
		}
	}

	if o.Fallback != "" {
		for _, flag := range strings.Split(o.Fallback, "|") {
			if _, ok := fallbackFlags[flag]; !ok {
				return ErrUnknownFallbackFlag
			}
		}
	} else {
		o.Fallback = "fb-default"
	}

	if len(o.LbMethod) == 0 {
		// WRR since Pulse will dynamically reweight backends.
		o.LbMethod = "wrr"
	}

	if o.MaxWeight <= 0 {
		o.MaxWeight = 100
	}

	if len(o.FwdMethod) == 0 {
		o.FwdMethod = "nat"
	}

	o.FwdMethod = strings.ToLower(o.FwdMethod)

	switch o.FwdMethod {
	case "dr":
		o.methodID = gnl2go.IPVS_DIRECTROUTE
	case "nat":
		o.methodID = gnl2go.IPVS_MASQUERADING
	case "tunnel", "ipip":
		o.methodID = gnl2go.IPVS_TUNNELING
	default:
		return ErrUnknownMethod
	}

	if o.Pulse == nil {
		// It doesn't make much sense to have a backend with no Pulse.
		o.Pulse = &pulse.Options{}
	}

	return nil
}

func (o *ServiceOptions) CompareStoreOptions(options *ServiceOptions) bool {
	if o.Host != options.Host {
		return false
	}
	if o.Port != options.Port {
		return false
	}
	if o.Protocol != options.Protocol {
		return false
	}
	if o.ShFlags != options.ShFlags {
		return false
	}
	if o.LbMethod != options.LbMethod {
		return false
	}
	if o.Persistent != options.Persistent {
		return false
	}
	if o.Fallback != options.Fallback {
		return false
	}
	if o.FwdMethod != options.FwdMethod {
		return false
	}
	if o.MaxWeight != options.MaxWeight {
		return false
	}
	return true
}

// BackendOptions describe a virtual service backend.
type BackendOptions struct {
	Host string `json:"host" yaml:"host"`
	Port uint16 `json:"port" yaml:"port"`

	// vsID of backend
	vsID string
	// Host string resolved to an IP, including DNS lookup.
	host net.IP
	// Backend current weight
	weight int32
	// pulse settings
	pulse *pulse.Options
}

// Validate fills missing fields and validates backend configuration.
func (o *BackendOptions) Validate() error {
	if len(o.Host) == 0 || o.Port == 0 {
		return ErrMissingEndpoint
	}

	if addr, err := net.ResolveIPAddr("ip", o.Host); err == nil {
		o.host = addr.IP
	} else {
		return err
	}

	return nil
}

func (o *BackendOptions) CompareStoreOptions(options *BackendOptions) bool {
	if o.Host != options.Host {
		return false
	}
	if o.Port != options.Port {
		return false
	}
	return true
}
