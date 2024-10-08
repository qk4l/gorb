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

package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/qk4l/gorb/core"
	"github.com/qk4l/gorb/util"

	"github.com/gorilla/mux"
)

// possible api errors
var (
	operationNotSupportedStore = errors.New("operation not supported with store")
)

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, obj interface{}) {
	w.Header().Add("Content-Type", "application/json")
	w.Write(util.MustMarshal(obj, util.JSONOptions{Indent: true}))
}

func writeError(w http.ResponseWriter, err error) {
	var code int

	switch err {
	case core.ErrIpvsSyscallFailed:
		code = http.StatusInternalServerError
	case core.ErrObjectExists:
		code = http.StatusConflict
	case core.ErrObjectNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusBadRequest
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(util.MustMarshal(&errorResponse{err.Error()}, util.JSONOptions{Indent: true}))
}

type serviceCreateHandler struct {
	ctx *core.Context
}

func (h serviceCreateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		serviceConfig core.ServiceConfig
		vars          = mux.Vars(r)
	)
	if h.ctx.StoreExist() {
		writeError(w, operationNotSupportedStore)
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&serviceConfig); err != nil {
		writeError(w, err)
	} else if err := h.ctx.CreateService(vars["vsID"], &serviceConfig); err != nil {
		writeError(w, err)
	}
}

type backendCreateHandler struct {
	ctx *core.Context
}

func (h backendCreateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		opts core.BackendOptions
		vars = mux.Vars(r)
	)

	if h.ctx.StoreExist() {
		writeError(w, operationNotSupportedStore)
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
		writeError(w, err)
	} else if err := h.ctx.CreateBackend(vars["vsID"], vars["rsID"], &opts); err != nil {
		writeError(w, err)
	}
}

type serviceRemoveHandler struct {
	ctx *core.Context
}

func (h serviceRemoveHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if h.ctx.StoreExist() {
		writeError(w, operationNotSupportedStore)
		return
	}

	if _, err := h.ctx.RemoveService(vars["vsID"]); err != nil {
		writeError(w, err)
	}
}

type backendRemoveHandler struct {
	ctx *core.Context
}

func (h backendRemoveHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if h.ctx.StoreExist() {
		writeError(w, operationNotSupportedStore)
		return
	}

	if _, err := h.ctx.RemoveBackend(vars["vsID"], vars["rsID"]); err != nil {
		writeError(w, err)
	}
}

type serviceListHandler struct {
	ctx *core.Context
}

func (h serviceListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if list, err := h.ctx.ListServices(); err != nil {
		writeError(w, err)
	} else {
		writeJSON(w, list)
	}
}

type serviceStatusHandler struct {
	ctx *core.Context
}

func (h serviceStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if opts, err := h.ctx.GetService(vars["vsID"]); err != nil {
		writeError(w, err)
	} else {
		writeJSON(w, opts)
	}
}

type backendStatusHandler struct {
	ctx *core.Context
}

func (h backendStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if opts, err := h.ctx.GetBackend(vars["vsID"], vars["rsID"]); err != nil {
		writeError(w, err)
	} else {
		writeJSON(w, opts)
	}
}

type storeSyncHandler struct {
	store *core.Store
}

func (h storeSyncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.store != nil {
		if err := h.store.StartSyncWithStore(); err != nil {
			writeError(w, err)
		} else {
			writeJSON(w, map[string]string{"status": "ok"})
		}
	} else {
		writeError(w, core.ErrObjectNotFound)
	}

}

type storeSyncStatusHandler struct {
	store *core.Store
}

func (h storeSyncStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.store != nil {
		if syncStatus, err := h.store.StoreSyncStatus(); err != nil {
			writeError(w, err)
		} else {
			writeJSON(w, syncStatus)
		}
	} else {
		writeError(w, core.ErrObjectNotFound)
	}

}
