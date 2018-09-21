package appengine

import (
	"fmt"
	"net/http"
	"os"

	"github.com/googlegenomics/beacon-go/api"
)

func init() {
	jsonFile, err := os.Open("beacon.json")
	if err != nil {
		panic(fmt.Sprintf("opening beacon json file: %v", err))
	}
	server, err := api.NewServerFromJson(os.Getenv("GOOGLE_CLOUD_PROJECT"), jsonFile)
	if err != nil {
		panic(fmt.Sprintf("creating a server instance: %v", err))
	}
	mux := http.NewServeMux()
	server.Export(mux)
	http.HandleFunc("/", mux.ServeHTTP)
}
