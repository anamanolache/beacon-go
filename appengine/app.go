package appengine

import (
	"fmt"
	"net/http"
	"os"

	"github.com/googlegenomics/beacon-go/beacon"
)

func init() {
	beaconInfo := beacon.BeaconInfo{
		ID:         mandatoryEnvVar("BEACON_ID"),
		Name:       mandatoryEnvVar("BEACON_NAME"),
		ApiVersion: mandatoryEnvVar("BEACON_API_VERSION"),
		Organization: beacon.OrganizationInfo{
			ID:   mandatoryEnvVar("ORGANIZATION_ID"),
			Name: mandatoryEnvVar("ORGANIZATION_NAME"),
		},
		Datasets: mandatoryEnvVar("GOOGLE_BIGQUERY_TABLE"),
	}

	beaconAPI := beacon.BeaconAPI{
		BeaconInfo: beaconInfo,
		ProjectID:  mandatoryEnvVar("GOOGLE_CLOUD_PROJECT"),
	}

	http.HandleFunc("/", beaconAPI.About)
	http.HandleFunc("/query", beaconAPI.Query)
}

func mandatoryEnvVar(name string) string {
	val := os.Getenv(name)
	if val == "" {
		panic(fmt.Sprintf("environment variable %s must be specified", name))
	}
	return val
}
