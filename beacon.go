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

// Package beacon implements a GA4GH Beacon (http://ga4gh.org/#/beacon) backed
// by the Google Genomics Variants service search API.
package beacon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"google.golang.org/appengine"
)

type beaconConfig struct {
	ID           string             `json:"id"`
	Name         string             `json:"name"`
	ApiVersion   string             `json:"apiVersion"`
	Organization beaconOrganization `json:"organization"`
	Datasets     string             `json:"datasets"`
}

type beaconOrganization struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

const (
	projectKey = "GOOGLE_CLOUD_PROJECT"
	bqTableKey = "GOOGLE_BIGQUERY_TABLE"
)

var (
	projectID = os.Getenv(projectKey)
	beacon    = beaconConfig{
		ID:         os.Getenv("BEACON_ID"),
		Name:       os.Getenv("BEACON_NAME"),
		ApiVersion: os.Getenv("BEACON_API_VERSION"),
		Organization: beaconOrganization{
			ID:   os.Getenv("ORGANIZATION_ID"),
			Name: os.Getenv("ORGANIZATION_NAME"),
		},
		Datasets: os.Getenv(bqTableKey),
	}
)

func init() {
	http.HandleFunc("/", aboutBeacon)
	http.HandleFunc("/query", query)
}

func aboutBeacon(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, fmt.Sprintf("HTTP method %s not supported", r.Method), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	response, err := json.Marshal(beacon)
	if err != nil {
		http.Error(w, fmt.Sprintf("writing response: %v", err), http.StatusInternalServerError)
	}
	w.Write(response)
}

func query(w http.ResponseWriter, r *http.Request) {
	if err := validateServerConfig(); err != nil {
		http.Error(w, fmt.Sprintf("validating server configuration: %v", err), http.StatusInternalServerError)
		return
	}

	request, err := parseInput(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("parsing input: %v", err), http.StatusBadRequest)
		return
	}
	query := &Query{
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

	if err := query.ValidateInput(); err != nil {
		writeError(w, *request, http.StatusBadRequest, fmt.Sprintf("validating input: %v", err))
		return
	}

	ctx := appengine.NewContext(r)
	exists, err := query.Execute(ctx, projectID, beacon.Datasets)
	if err != nil {
		writeError(w, *request, http.StatusInternalServerError, fmt.Sprintf("computing result: %v", err))
		return
	}
	writeResponse(w, *request, exists)
}

func validateServerConfig() error {
	if projectID == "" {
		return fmt.Errorf("%s must be specified", projectKey)
	}
	if beacon.Datasets == "" {
		return fmt.Errorf("%s must be specified", bqTableKey)
	}
	return nil
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

func writeError(w http.ResponseWriter, req alleleRequest, code int, message string) {
	write(w, req, nil, &beaconError{
		Code:    strconv.Itoa(code),
		Message: message,
	})
}

func writeResponse(w http.ResponseWriter, req alleleRequest, exists bool) {
	write(w, req, &exists, nil)
}

func write(w http.ResponseWriter, req alleleRequest, exists *bool, beaconErr *beaconError) {
	response := alleleResponse{
		BeaconId:   beacon.ID,
		ApiVersion: beacon.ApiVersion,
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
