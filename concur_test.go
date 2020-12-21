package concur

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"testing"
)

//It is too slow. You have failed. Try this on a giant file to see if it has any purpose.

type pblog struct {
	ReplayID   int64               `json:"ReplayID"`
	DateSent   int64               `json:"DateSent"`
	ReqHeaders map[string][]string `json:"RequestHeaders,omitempty"`
	ReqURL     string              `json:"RequestURL"`
	ReqMethod  string              `json:"RequestMethod"`
	ReqBody    string              `json:"RequestBody,omitempty"`
}

//Time to write up a test for this bad boy.
//Just need a json file to work with, then I'll print out the resulting objects.
//Make a standard file read test to run alongside this

func TestReadConcur(t *testing.T) {

	var request pblog
	jobs := make(chan []byte)

	fmt.Println("Running File Read Test")
	//100000000 appears to be too large to
	go ReadFileConcur("remainingRequests.20200803152110.json", 50000000, 20, nil, jobs)

	i := 0
	for j := range jobs {

		i++
		fmt.Println("Unmarshalling line")
		err := json.Unmarshal(bytes.Trim(j, "[,]"), &request)
		fmt.Println(request)
		if err != nil {
			log.Println("Failed to Unmarshal:")
			log.Println("[ERROR]: ", err)
			fmt.Println("ERROR: " + string(j))
		}
	}

	fmt.Printf("%d requests processed\n", i)
}

func TestReadNormal(t *testing.T) {

	var requests []pblog
	path := "remainingRequests.20200803152110.json"

	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}
	err = json.Unmarshal(dat, &requests)
	if err != nil {
		return
	}

	i := 0
	for _, r := range requests {

		i++
		fmt.Println(r)
	}

	fmt.Printf("%d requests processed\n", i)
}
