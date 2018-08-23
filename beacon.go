/*
 * Copyright (C) 2018 Google Inc.
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
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"cloud.google.com/go/bigquery"
	"google.golang.org/appengine"
)

type beaconConfig struct {
	ApiVersion string
	ProjectID  string
	TableId    string
}

const (
	apiVersionKey = "VERSION"
	projectKey    = "GOOGLE_CLOUD_PROJECT"
	bqTableKey    = "GOOGLE_BIGQUERY_TABLE"
)

var (
	aboutTemplate = template.Must(template.ParseFiles("about.xml"))
	config        = beaconConfig{
		ApiVersion: os.Getenv(apiVersionKey),
		ProjectID:  os.Getenv(projectKey),
		TableId:    os.Getenv(bqTableKey),
	}
)

func init() {
	http.HandleFunc("/", aboutBeacon)
	http.HandleFunc("/query", query)
}

func aboutBeacon(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	aboutTemplate.Execute(w, config)
}

func query(w http.ResponseWriter, r *http.Request) {
	if err := validateServerConfig(); err != nil {
		http.Error(w, fmt.Sprintf("validating server configuration: %v", err), http.StatusInternalServerError)
		return
	}

	params, err := parseInput(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("validating input: %v", err), http.StatusBadRequest)
		return
	}

	ctx := appengine.NewContext(r)
	exists, err := genomeExists(ctx, params)
	if err != nil {
		http.Error(w, fmt.Sprintf("computing result: %v", err), http.StatusInternalServerError)
		return
	}

	if err := writeResponse(w, exists); err != nil {
		http.Error(w, fmt.Sprintf("validating server configuration: %v", err), http.StatusInternalServerError)
		return
	}
}

func genomeExists(ctx context.Context, params queryParams) (bool, error) {
	var w where
	w.append(fmt.Sprintf("reference_name='%s'", params.RefName))
	w.append(bqCoordinatesToWhereClause(params))
	w.append(fmt.Sprintf("reference_bases='%s'", params.RefBases))

	query := fmt.Sprintf(`
		SELECT count(v.reference_name) as count
		FROM %s as v
		WHERE %s
		LIMIT 1`,
		fmt.Sprintf("`%s`", config.TableId),
		w.clause)

	bqclient, err := bigquery.NewClient(ctx, config.ProjectID)
	if err != nil {
		return false, fmt.Errorf("creating bigquery client: %v", err)
	}
	it, err := bqclient.Query(query).Read(ctx)
	if err != nil {
		return false, fmt.Errorf("querying database: %v", err)
	}

	var result struct {
		Count int
	}
	if err := it.Next(&result); err != nil {
		return false, fmt.Errorf("reading query result: %v", err)
	}
	return result.Count > 0, nil
}

type where struct {
	clause string
}

func (w *where) append(statement string) {
	if statement == "" {
		return
	}
	var conj string
	if len(w.clause) > 0 {
		conj = " AND "
	}
	w.clause = fmt.Sprintf("%s%s(%s)", w.clause, conj, statement)
}

func validateServerConfig() error {
	if config.ProjectID == "" {
		return fmt.Errorf("%s must be specified", projectKey)
	}
	if config.TableId == "" {
		return fmt.Errorf("%s must be specified", bqTableKey)
	}
	return nil
}

type queryParams struct {
	RefName  string `json:"referenceName"`
	RefBases string `json:"referenceBases"`
	Start    *int64 `json:"start"`
	End      *int64 `json:"end"`
}

func parseInput(r *http.Request) (queryParams, error) {
	var params queryParams
	if r.Method == "GET" {
		params.RefName = r.FormValue("referenceName")
		params.RefBases = r.FormValue("referenceBases")
		if err := parseCoordinates(r, &params); err != nil {
			return queryParams{}, fmt.Errorf("parsing referenceBases: %v", err)
		}
	} else if r.Method == "POST" {
		body, _ := ioutil.ReadAll(r.Body)
		if err := json.Unmarshal(body, &params); err != nil {
			return queryParams{}, fmt.Errorf("decoding request body: %v", err)
		}
	}

	if err := validateInput(params); err != nil {
		return queryParams{}, fmt.Errorf("validating input: %v", err)
	}
	return params, nil
}

func parseCoordinates(r *http.Request, params *queryParams) error {
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
	return nil
}

func validateInput(params queryParams) error {
	if params.RefName == "" {
		return errors.New("missing referenceName")
	}
	if params.RefBases == "" {
		return errors.New("missing referenceBases")
	}

	if err := validateCoordinates(params); err != nil {
		return fmt.Errorf("validating coordinates: %v", err)
	}
	return nil
}

func validateCoordinates(params queryParams) error {
	if params.Start != nil && (params.End != nil || params.RefBases != "") {
		return nil
	}
	return errors.New("coordinate requirements not met")
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

func bqCoordinatesToWhereClause(params queryParams) string {
	if params.Start != nil {
		if params.End != nil {
			return fmt.Sprintf("v.start = %d AND %d = v.end", *params.Start, *params.End)
		}
		return fmt.Sprintf("v.start = %d", *params.Start)
	}
	return ""
}

func writeResponse(w http.ResponseWriter, exists bool) error {
	type beaconResponse struct {
		XMLName struct{} `xml:"BEACONResponse"`
		Exists  bool     `xml:"exists"`
	}
	var resp beaconResponse
	resp.Exists = exists

	w.Header().Set("Content-Type", "application/xml")
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(resp); err != nil {
		return fmt.Errorf("serializing response: %v", err)
	}
	return nil
}
