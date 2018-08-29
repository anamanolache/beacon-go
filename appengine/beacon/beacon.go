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

// Package beacon provides request handlers for the GA4GH Beacon (http://ga4gh.org/#/beacon)
// endpoints.
package beacon

import (
	"net/http"

	"github.com/googlegenomics/beacon-go/internal/beacon"
)

func BeaconHandler() func(w http.ResponseWriter, req *http.Request) {
	return beacon.AboutBeacon
}

func QueryHandler() func(w http.ResponseWriter, req *http.Request) {
	return beacon.ExecuteQuery
}
