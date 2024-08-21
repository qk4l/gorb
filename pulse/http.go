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

package pulse

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/qk4l/gorb/util"

	log "github.com/sirupsen/logrus"
)

var (
	errRedirects = errors.New("redirects are not supported for pulse requests")
)

type httpPulse struct {
	Driver

	client http.Client
	httpRq *http.Request
	expect int
}

func newGETDriver(host string, port uint16, opts util.DynamicMap) (Driver, error) {
	log.Debugf("Create pulse for %s:%d", host, port)

	pulseScheme := opts.Get("scheme", "http").(string)
	pulseHost := opts.Get("host", host).(string)
	pulsePort := opts.Get("port", int(port)).(int)
	pulseTimeout := opts.Get("timeout", 2).(int)
	pulsePath := opts.Get("path", "/").(string)

	c := http.Client{}
	urlHost := fmt.Sprintf("%s:%d", pulseHost, pulsePort)

	if pulseScheme == "https" {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		c = http.Client{Timeout: time.Duration(pulseTimeout) * time.Second, Transport: tr, CheckRedirect: func(
			req *http.Request,
			via []*http.Request,
		) error {
			return errRedirects
		}}
		// Do not pass port to Host header
		if pulsePort == 443 {
			urlHost = pulseHost
		}

	} else {
		c = http.Client{Timeout: time.Duration(pulseTimeout) * time.Second, CheckRedirect: func(
			req *http.Request,
			via []*http.Request,
		) error {
			return errRedirects
		}}
		// Do not pass port to Host header
		if pulsePort == 80 {
			urlHost = pulseHost
		}
	}

	pulsePath_parsed, err := url.Parse(pulsePath)
	if err != nil {
		log.Errorf("failed to parse %s for backend %s", pulsePath, pulseHost)
		return nil, err
	}

	u := url.URL{
		Scheme:   pulseScheme,
		Host:     urlHost,
		Path:     pulsePath_parsed.EscapedPath(),
		RawQuery: pulsePath_parsed.RawQuery,
	}

	r, err := http.NewRequest(opts.Get("method", "GET").(string), u.String(), nil)
	if err != nil {
		return nil, err
	}

	return &httpPulse{
		client: c,
		httpRq: r,
		expect: opts.Get("expect", 200).(int),
	}, nil
}

func (p *httpPulse) Check() StatusType {
	if r, err := p.client.Do(p.httpRq); err != nil {
		log.Errorf("error while communicating with %s: %s", p.httpRq.URL, err)
	} else if r.StatusCode != p.expect {
		log.Errorf("received non-%d status code from %s", p.expect, p.httpRq.URL)
	} else {
		return StatusUp
	}

	return StatusDown
}
