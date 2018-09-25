package appengine

import (
	"fmt"
	"net/http"
	"os"

	"encoding/json"
	"github.com/googlegenomics/beacon-go/api"
	"io/ioutil"
)

func init() {
	server, err := newServer()
	if err != nil {
		panic(fmt.Sprintf("creating a server instance: %v", err))
	}
	mux := http.NewServeMux()
	server.Export(mux)
	http.HandleFunc("/", mux.ServeHTTP)
}

func newServer() (*api.Server, error) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		return nil, fmt.Errorf("the google cloud project id must be specified")
	}

	jsonFile, err := os.Open("beacon.json")
	if err == nil {
		return newServerFromJson(projectID, jsonFile)
	}

	dataset := os.Getenv("VARIANTS_DATASET")
	if dataset == "" {
		return nil, fmt.Errorf("either the beacon.json file or VARIANTS_DATASET variable must be present")
	}
	server := api.Server{
		BeaconInfo: api.Beacon{
			Dataset: dataset,
		},
		ProjectID: projectID,
	}
	return &server, nil
}

func newServerFromJson(projectID string, jsonFile *os.File) (*api.Server, error) {
	bytes, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, fmt.Errorf("reading beacon json: %v", err)
	}
	var beacon api.Beacon
	if err := json.Unmarshal(bytes, &beacon); err != nil {
		return nil, fmt.Errorf("parsing beacon from json: %v", err)
	}
	server := api.Server{
		BeaconInfo: beacon,
		ProjectID:  projectID,
	}
	return &server, nil
}
