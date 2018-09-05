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

// Package beacon contains an implementation of GA4GH Beacon API (http://ga4gh.org/#/beacon).
package beacon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/googlegenomics/beacon-go/internal/query"
	"google.golang.org/appengine"
)

// BeaconInfo contains information about the organization owning the beacon.
type BeaconInfo struct {
	// ID the unique identifier of the beacon.
	ID string `json:"id"`
	// Name the name of the beacon.
	Name string `json:"name"`
	// ApiVersion the version of the GA4GH Beacon specification the API implements.
	ApiVersion string `json:"apiVersion"`
	// Organization information about the organization that owns the beacon.
	Organization OrganizationInfo `json:"organization"`
	// Datasets the ID of the allele BigQuery table to query.
	// Must be provided in the following format: bigquery-project.dataset.table.
	Datasets string `json:"datasets"`
}

// OrganizationInfo contains information about an organization.
type OrganizationInfo struct {
	// ID the unique identifier of the organization.
	ID string `json:"id"`
	// Name the name of the organization.
	Name string `json:"name"`
}

// BeaconAPI implements a GA4GH Beacon API (http://ga4gh.org/#/beacon) backed
// by a Google Cloud BigQuery allele table.
type BeaconAPI struct {
	// BeaconInfo information about the beacon implementation.
	BeaconInfo BeaconInfo
	// ProjectID the GCloud project ID.
	ProjectID string
}

func (api *BeaconAPI) About(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, fmt.Sprintf("HTTP method %s not supported", r.Method), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	response, err := json.Marshal(api.BeaconInfo)
	if err != nil {
		http.Error(w, fmt.Sprintf("writing response: %v", err), http.StatusInternalServerError)
	}
	w.Write(response)
}

func (api *BeaconAPI) Query(w http.ResponseWriter, r *http.Request) {
	request, err := parseInput(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("parsing input: %v", err), http.StatusBadRequest)
		return
	}
	q := &query.Query{
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
	exists, err := q.Execute(ctx, api.ProjectID, api.BeaconInfo.Datasets)

	if err != nil {
		api.writeError(w, *request, http.StatusInternalServerError, fmt.Sprintf("computing result: %v", err))
		return
	}
	api.writeResponse(w, *request, exists)
}

type alleleRequest struct {
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

func parseInput(r *http.Request) (*alleleRequest, error) {
	if r.Method == "GET" {
		var query alleleRequest
		query.ReferenceName = r.FormValue("referenceName")
		query.ReferenceBases = r.FormValue("referenceBases")
		query.AlternateBases = r.FormValue("alternateBases")
		if err := parseFormCoordinates(r, &query); err != nil {
			return nil, fmt.Errorf("parsing referenceBases: %v", err)
		}
		return &query, nil
	} else if r.Method == "POST" {
		var params alleleRequest
		body, _ := ioutil.ReadAll(r.Body)
		if err := json.Unmarshal(body, &params); err != nil {
			return nil, fmt.Errorf("decoding request body: %v", err)
		}
		return &params, nil
	}
	return nil, errors.New(fmt.Sprintf("HTTP method %s not supported", r.Method))
}

func parseFormCoordinates(r *http.Request, params *alleleRequest) error {
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
		return nil, fmt.Errorf("parsing int value: %v", err)
	}
	return &value, nil
}

type alleleResponse struct {
	BeaconId   string        `json:"beaconId"`
	ApiVersion string        `json:"apiVersion"`
	Request    alleleRequest `json:"request"`
	Exists     *bool         `json:"exists"`
	Error      *beaconError  `json:"error"`
}

type beaconError struct {
	Code    string `json:"errorCode"`
	Message string `json:"errorMessage"`
}

func (api *BeaconAPI) writeError(w http.ResponseWriter, req alleleRequest, code int, message string) {
	api.write(w, req, nil, &beaconError{
		Code:    strconv.Itoa(code),
		Message: message,
	})
}

func (api *BeaconAPI) writeResponse(w http.ResponseWriter, req alleleRequest, exists bool) {
	api.write(w, req, &exists, nil)
}

func (api *BeaconAPI) write(w http.ResponseWriter, req alleleRequest, exists *bool, beaconErr *beaconError) {
	response := alleleResponse{
		BeaconId:   api.BeaconInfo.ID,
		ApiVersion: api.BeaconInfo.ApiVersion,
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
