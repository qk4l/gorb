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
	"github.com/qk4l/gorb/pulse"
	log "github.com/sirupsen/logrus"
)

func (ctx *Context) run() {
	stash := make(map[pulse.ID]int32)

	for {
		select {
		case u := <-ctx.pulseCh:
			ctx.processPulseUpdate(stash, u)
		case <-ctx.stopCh:
			log.Debug("notificationLoop has been stopped")
			return
		}
	}
}

func (ctx *Context) processPulseUpdate(stash map[pulse.ID]int32, u pulse.Update) {
	vsID, rsID := u.Source.VsID, u.Source.RsID
	ctx.mutex.Lock()
	// check exist
	vs, ok := ctx.services[vsID]
	if !ok {
		if _, exists := stash[u.Source]; exists {
			log.Debugf("service %s has been deleted, so deleting it from stash too", u.Source)
			delete(stash, u.Source)
		}
		ctx.mutex.Unlock()
		return
	}
	rs, ok := vs.backends[rsID]

	if !ok || u.Metrics.Status == pulse.StatusRemoved {
		if _, exists := stash[u.Source]; exists {
			log.Debugf("backend %s has been deleted, so deleting it from stash too", u.Source)
			delete(stash, u.Source)
		}
		ctx.mutex.Unlock()
		return
	}

	if rs.metrics.Status != u.Metrics.Status {
		log.Warnf("backend %s status: %s", u.Source, u.Metrics.Status)
	}
	// This is a copy of metrics structure from Pulse.
	rs.metrics = u.Metrics

	ctx.mutex.Unlock()

	switch u.Metrics.Status {
	case pulse.StatusUp:
		// Weight is gonna be stashed until the backend is recovered.
		weight, exists := stash[u.Source]

		if !exists {
			return
		}

		// Calculate a relative weight considering backend's health.
		weight = int32(float64(weight) * u.Metrics.Health)

		if _, err := ctx.UpdateBackend(vsID, rsID, weight); err != nil {
			log.Errorf("error while unstashing a backend: %s", err)
		} else if weight == stash[u.Source] {
			log.Infof("backend %s has completely recovered, so deleting it from stash.", u.Source)
			// This means that the backend has completely recovered.
			delete(stash, u.Source)
		}

	case pulse.StatusDown:
		// Always set backend weight to 0 if StatusDown
		backendWeight := int32(0)

		// Apply Fallback rules
		if serviceInfo, err := ctx.GetService(vsID); err != nil {
			log.Errorf("error while getting service info for %s: %s", vsID, err)
		} else {
			if serviceInfo.Health == 0 {
				switch fallbackFlags[serviceInfo.FallBack] {
				case ZeroToOne:
					backendWeight++
					log.Infof("service %s has zero health. use %s fallback strategy", vsID, serviceInfo.FallBack)
				default:
					log.Infof("use default fallback strategy")
				}
			}
		}

		if weight, err := ctx.UpdateBackend(vsID, rsID, backendWeight); err != nil {
			log.Errorf("error while stashing a backend: %s", err)
		} else {
			if _, exists := stash[u.Source]; exists {
				return
			}
			stash[u.Source] = weight
		}
	}
}
