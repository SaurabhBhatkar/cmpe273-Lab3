package main

import (
	"bytes"

	"encoding/json"
	"fmt"

	"io/ioutil"
	"net/http"

	"github.com/cznic/sortutil"
	"github.com/drone/routes"
	"github.com/spaolacci/murmur3"
)

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var nodeMap map[uint64]string
var keys sortutil.Uint64Slice

func GetVal(resw http.ResponseWriter, r *http.Request) {

	key := r.URL.Query().Get(":keyID")
	address := getNode(key)
	var buffer bytes.Buffer
	buffer.WriteString(address)
	buffer.WriteString("keys")
	buffer.WriteString("/")
	buffer.WriteString(key)

	req, err := http.NewRequest("GET", buffer.String(), nil)
	if err != nil {
		fmt.Println("error: body, _ := ioutil.ReadAll(resp.Body)")
		panic(err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("ERROR: Cannot submit request")
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var response Response

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("ERROR while reading body")
			panic(err.Error())
		}

		err = json.Unmarshal(body, &response)
		if err != nil {
			fmt.Println("ERROR: Cannot Unmarshal JSON")
			panic(err.Error())
		}
		resw.WriteHeader(http.StatusOK)
		resw.Header().Set("Content-Type", "application/json")
		outputJSON, err := json.Marshal(response)
		if err != nil {
			resw.Write([]byte(`{    "error": "Unable to marshal response.`))
			panic(err.Error())
		}
		fmt.Println("Retrieved from node : ", address, " for key : ", key)
		resw.Write(outputJSON)
	} else {
		resw.WriteHeader(resp.StatusCode)
	}

}

func InsertVal(resw http.ResponseWriter, r *http.Request) {

	fmt.Println("Value Enter:")
	key := r.URL.Query().Get(":keyID")
	value := r.URL.Query().Get(":value")
	address := getNode(key)
	var buffer bytes.Buffer
	buffer.WriteString(address)
	buffer.WriteString("keys")
	buffer.WriteString("/")
	buffer.WriteString(key)
	buffer.WriteString("/")
	buffer.WriteString(value)

	req, err := http.NewRequest("PUT", buffer.String(), nil)
	if err != nil {
		fmt.Println("error: body, _ := ioutil.ReadAll(resp.Body)")
		panic(err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("ERROR: Cannot Process Put Request")

		panic(err)
	}
	defer resp.Body.Close()
	resw.WriteHeader(resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Inconsistent Data")
		panic(err.Error())
	} else {
		fmt.Println("Key : ", key, " || Value : ", value, " --- added to node : ", address)
	}

}

func getNode(key string) string {
	keyHash := murmur3.Sum64([]byte(key))
	var rIndex = len(keys) - 1
	for index, element := range keys {
		if keyHash < element {
			if index > 0 {
				rIndex = index - 1
			}
			break

		}
	}
	return nodeMap[keys[rIndex]]

}

func main() {
	nodeMap = make(map[uint64]string)
	node1 := "http://localhost:3000/"
	node2 := "http://localhost:3001/"
	node3 := "http://localhost:3002/"

	//Sort the map

	keys = append(keys, murmur3.Sum64([]byte(node1)))
	keys = append(keys, murmur3.Sum64([]byte(node2)))
	keys = append(keys, murmur3.Sum64([]byte(node3)))

	keys.Sort()
	fmt.Println("Keys array is : ", keys)

	for _, element := range keys {
		switch element {

		case murmur3.Sum64([]byte(node1)):
			nodeMap[element] = node1
		case murmur3.Sum64([]byte(node2)):
			nodeMap[element] = node2
		case murmur3.Sum64([]byte(node3)):
			nodeMap[element] = node3
		}

	}

	mux := routes.New()
	mux.Put("/keys/:keyID/:value", InsertVal)
	mux.Get("/keys/:keyID", GetVal)

	http.Handle("/", mux)
	http.ListenAndServe(":8090", nil)

}
