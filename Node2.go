package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/drone/routes"
)

var cacheMap map[string]string

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type AllResponse struct {
	AllValues []Response `json:"allValues"`
}

func GetVal(resw http.ResponseWriter, r *http.Request) {

	key := r.URL.Query().Get(":keyID")
	var response Response
	isKeyFound := false
	fmt.Println("Key to search for is : ", key)

	if cacheMap == nil {
		fmt.Println("Making new map")
		cacheMap = make(map[string]string)
		resw.WriteHeader(http.StatusNotFound)
		resw.Write([]byte(`{"error": "Cache is empty"}`))
	} else {
		fmt.Println("Entered else part")
		for mapKey, mapVal := range cacheMap {
			fmt.Println(key, "  ", mapKey)
			if key == mapKey {
				fmt.Println("Entered if part")
				resw.Header().Set("Content-Type", "application/json")
				resw.WriteHeader(http.StatusOK)
				response.Key = mapKey
				response.Value = mapVal
				outputJSON, err := json.Marshal(response)
				if err != nil {
					fmt.Println(err)
					panic(err)
				}
				resw.Write(outputJSON)
				isKeyFound = true
				break
			}
		}
		if !isKeyFound {
			resw.WriteHeader(http.StatusNotFound)
			resw.Write([]byte(`{"error": "value not found"}`))
		}
		fmt.Println(isKeyFound)

	}

}

func InsertVal(resw http.ResponseWriter, r *http.Request) {

	key := r.URL.Query().Get(":keyID")
	value := r.URL.Query().Get(":value")

	if cacheMap == nil {
		cacheMap = make(map[string]string)
	}
	cacheMap[key] = value
	fmt.Println(cacheMap)
	resw.WriteHeader(http.StatusOK)

}

func GetAllVal(resw http.ResponseWriter, r *http.Request) {
	var response AllResponse
	var currValue Response
	for mapKey, mapVal := range cacheMap {
		currValue.Key = mapKey
		currValue.Value = mapVal
		response.AllValues = append(response.AllValues, currValue)
	}
	resw.WriteHeader(http.StatusOK)
	outputJSON, _ := json.Marshal(response)
	resw.Header().Set("Content-Type", "application/json")
	resw.Write(outputJSON)
}

func main() {

	mux := routes.New()
	mux.Put("/keys/:keyID/:value", InsertVal)

	mux.Get("/keys/:keyID", GetVal)
	mux.Get("/keys", GetAllVal)

	http.Handle("/", mux)
	http.ListenAndServe(":3001", nil)
}
