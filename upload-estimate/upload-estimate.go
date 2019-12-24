package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

type JsonFormat struct {
	NeedBackup int `json:"needBackup"`
	NoBackup   int `json:"noBackup"`
}

type IndexRecord struct {
	NeedBackup int       `json:"needBackup"`
	NoBackup   int       `json:"noBackup"`
	Timestamp  time.Time `json:"timestamp"`
}

/**
takes data from the source and outputs an IndexRecord and a document ID (as a string)
arguments:
  - fromData: A pointer to a JsonFormat object containing the data to use
*/
func MakeIndexRecord(fromData *JsonFormat) (IndexRecord, string) {
	return IndexRecord{
		NeedBackup: fromData.NeedBackup,
		NoBackup:   fromData.NoBackup,
		Timestamp:  time.Now(),
	}, strconv.FormatInt(time.Now().UnixNano(), 10)
}

func GetDefaultFilename() string {
	var basePath = os.Getenv("HOME")
	if basePath == "" {
		basePath = "/tmp"
	}
	return basePath + "/backup-estimate.json"
}

func connectToES(elasticUrlPtr *string, indexNamePtr *string) *elasticsearch.Client {
	cfg := elasticsearch.Config{
		Addresses: []string{
			*elasticUrlPtr,
		},
	}

	client, cliErr := elasticsearch.NewClient(cfg)

	if cliErr != nil {
		log.Fatalf("Could not connect to Elasticsearch at %s: %s", *elasticUrlPtr, cliErr)
	}
	return client
}

func LoadFile(fileNamePtr *string) (*JsonFormat, error) {
	var content JsonFormat
	f, openErr := os.Open(*fileNamePtr)
	if openErr != nil {
		log.Printf("Could not open %s to read: %s", *fileNamePtr, openErr)
		return nil, openErr
	}
	defer f.Close()

	data, readErr := ioutil.ReadAll(f)
	if readErr != nil {
		log.Printf("Could not read data from %s: %s", *fileNamePtr, openErr)
		return nil, readErr
	}

	err := json.Unmarshal(data, &content)

	if err != nil {
		log.Printf("Could not parse data from %s: %s", *fileNamePtr, err)
	}
	return &content, nil
}

func main() {
	defaultFileName := GetDefaultFilename()
	fileNamePtr := flag.String("input-file", defaultFileName, "Name of the json file to import and upload")
	elasticUrlPtr := flag.String("elasticsearch", "http://localhost:9200", "URL to the Elasticsearch cluster")
	indexNamePtr := flag.String("index", "backup-estimate", "Name of the index to save data to")

	flag.Parse()
	esClient := connectToES(elasticUrlPtr, fileNamePtr)

	log.Printf("filename is %s", *fileNamePtr)
	content, loadErr := LoadFile(fileNamePtr)
	if loadErr != nil {
		os.Exit(1)
	}

	rec, docId := MakeIndexRecord(content)
	outputContent, marshalErr := json.Marshal(rec)
	if marshalErr != nil {
		log.Fatal("Could not marshal output content: ", marshalErr)
	}

	req := esapi.IndexRequest{
		Index:      *indexNamePtr,
		DocumentID: docId,
		Body:       bytes.NewReader(outputContent),
		Refresh:    "true",
	}

	result, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Fatal("Could not output data to ES: ", err)
	}
	defer result.Body.Close()

	if result.IsError() {
		log.Fatal("Could not output data to ES: ", result.Status())
	} else {
		log.Printf("Output record to ES")
	}
}
