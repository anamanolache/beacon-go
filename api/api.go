/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

// Package api implements a GA4GH Beacon API (https://github.com/ga4gh-beacon/specification/blob/master/beacon.md).
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/googlegenomics/beacon-go/internal/variants"
	"google.golang.org/appengine"
)

const beaconAPIVersion = "v0.0.1"

// Server provides handlers for Beacon API requests.
type Server struct {
	// BeaconInfo contains information about the beacon implementation.
	BeaconInfo Beacon
	// ProjectID is the GCloud project ID.
	ProjectID string
}

// Export registers the beacon API endpoint with mux.
func (server *Server) Export(mux *http.ServeMux) {
	mux.Handle("/", forwardOrigin(server.About))
	mux.Handle("query", forwardOrigin(server.Query))
}

// About retrieves all the necessary information on the beacon and the API.
func (api *Server) About(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, fmt.Sprintf("HTTP method %s not supported", r.Method), http.StatusBadRequest)
		return
	}

	api.BeaconInfo.APIVersion = beaconAPIVersion
	w.Header().Set("Content-Type", "application/json")
	response, err := json.Marshal(api.BeaconInfo)
	if err != nil {
		http.Error(w, fmt.Sprintf("writing response: %v", err), http.StatusInternalServerError)
	}
	w.Write(response)
}

// Query retrieves whether the requested allele exists in the dataset.
func (api *Server) Query(w http.ResponseWriter, r *http.Request) {
	request, err := parseInput(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("parsing input: %v", err), http.StatusBadRequest)
		return
	}
	q := &variants.Query{
		ReferenceName:  request.ReferenceName,
		ReferenceBases: request.ReferenceBases,
		AlternateBases: request.AlternateBases,
		Start:          request.Start,
		End:            request.End,
		StartMin:       request.StartMin,
		StartMax:       request.StartMax,
		EndMin:         request.EndMin,
		EndMax:         request.EndMax,
	}

	if err := q.ValidateInput(); err != nil {
		api.writeError(w, *request, http.StatusBadRequest, fmt.Sprintf("validating input: %v", err))
		return
	}

	ctx := appengine.NewContext(r)
	exists, err := q.Execute(ctx, api.ProjectID, api.BeaconInfo.Dataset)

	if err != nil {
		api.writeError(w, *request, http.StatusInternalServerError, fmt.Sprintf("computing result: %v", err))
		return
	}
	api.writeResponse(w, *request, exists)
}

// Beacon contains information about the beacon implementation.
type Beacon struct {
	// ID the unique identifier of the beacon.
	ID string `json:"id"`
	// Name the name of the beacon.
	Name string `json:"name"`
	// APIVersion the version of the GA4GH Beacon specification the API implements.
	APIVersion string `json:"apiVersion"`
	// BeaconOrganization contains information about the organization owning the beacon.
	Organization BeaconOrganization `json:"organization"`
	// Dataset the ID of the allele BigQuery table to query.
	// Must be provided in the following format: bigquery-project.dataset.table.
	Dataset string `json:"dataset"`
}

// BeaconOrganization contains information about the organization owning the beacon.
type BeaconOrganization struct {
	// ID the unique identifier of the organization.
	ID string `json:"id"`
	// Name the name of the organization.
	Name string `json:"name"`
}

type beaconAlleleRequest struct {
	ReferenceName  string `json:"referenceName"`
	ReferenceBases string `json:"referenceBases"`
	AlternateBases string `json:"alternateBases"`
	Start          *int64 `json:"start"`
	End            *int64 `json:"end"`
	StartMin       *int64 `json:"startMin"`
	StartMax       *int64 `json:"startMax"`
	EndMin         *int64 `json:"endMin"`
	EndMax         *int64 `json:"endMax"`
}

type beaconAlleleResponse struct {
	BeaconId   string              `json:"beaconId"`
	ApiVersion string              `json:"apiVersion"`
	Request    beaconAlleleRequest `json:"alleleRequest"`
	Exists     *bool               `json:"exists"`
	Error      *beaconError        `json:"error"`
}

type beaconError struct {
	Code    string `json:"errorCode"`
	Message string `json:"errorMessage"`
}

func parseInput(r *http.Request) (*beaconAlleleRequest, error) {
	if r.Method == "GET" {
		var query beaconAlleleRequest
		query.ReferenceName = r.FormValue("referenceName")
		query.ReferenceBases = r.FormValue("referenceBases")
		query.AlternateBases = r.FormValue("alternateBases")
		if err := parseFormCoordinates(r, &query); err != nil {
			return nil, fmt.Errorf("parsing referenceBases: %v", err)
		}
		return &query, nil
	} else if r.Method == "POST" {
		var params beaconAlleleRequest
		body, _ := ioutil.ReadAll(r.Body)
		if err := json.Unmarshal(body, &params); err != nil {
			return nil, fmt.Errorf("decoding request body: %v", err)
		}
		return &params, nil
	}
	return nil, errors.New(fmt.Sprintf("HTTP method %s not supported", r.Method))
}

func parseFormCoordinates(r *http.Request, params *beaconAlleleRequest) error {
	start, err := getFormValueInt(r, "start")
	if err != nil {
		return fmt.Errorf("parsing start: %v", err)
	}
	params.Start = start

	end, err := getFormValueInt(r, "end")
	if err != nil {
		return fmt.Errorf("parsing end: %v", err)
	}
	params.End = end

	startMin, err := getFormValueInt(r, "startMin")
	if err != nil {
		return fmt.Errorf("parsing startMin: %v", err)
	}
	params.StartMin = startMin

	startMax, err := getFormValueInt(r, "startMax")
	if err != nil {
		return fmt.Errorf("parsing startMax: %v", err)
	}
	params.StartMax = startMax

	endMin, err := getFormValueInt(r, "endMin")
	if err != nil {
		return fmt.Errorf("parsing endMin: %v", err)
	}
	params.EndMin = endMin

	endMax, err := getFormValueInt(r, "endMax")
	if err != nil {
		return fmt.Errorf("parsing endMax: %v", err)
	}
	params.EndMax = endMax
	return nil
}

func getFormValueInt(r *http.Request, key string) (*int64, error) {
	str := r.FormValue(key)
	if str == "" {
		return nil, nil
	}
	value, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing value as integer: %v", err)
	}
	return &value, nil
}

func (api *Server) writeError(w http.ResponseWriter, req beaconAlleleRequest, code int, message string) {
	api.write(w, req, nil, &beaconError{
		Code:    strconv.Itoa(code),
		Message: message,
	})
}

func (api *Server) writeResponse(w http.ResponseWriter, req beaconAlleleRequest, exists bool) {
	api.write(w, req, &exists, nil)
}

func (api *Server) write(w http.ResponseWriter, req beaconAlleleRequest, exists *bool, beaconErr *beaconError) {
	response := beaconAlleleResponse{
		BeaconId:   api.BeaconInfo.ID,
		ApiVersion: api.BeaconInfo.APIVersion,
		Request:    req,
		Exists:     exists,
		Error:      beaconErr,
	}
	w.Header().Set("Content-Type", "application/json")
	buffer, err := json.Marshal(response)
	if err != nil {
		http.Error(w, fmt.Sprintf("writing response: %v", err), http.StatusInternalServerError)
	}
	w.Write(buffer)
}

type forwardOrigin func(w http.ResponseWriter, req *http.Request)

func (f forwardOrigin) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if origin := req.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	f(w, req)
}
