package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type JsonFormat struct {
	NeedBackup     int   `json:"needsBackupCount"`
	NeedBackupSize int64 `json:"needsBackupSize"`
	NoBackup       int   `json:"noBackupCount"`
	NoBackupSize   int64 `json:"noBackupSize"`
}

type IndexRecord struct {
	NeedBackup     int       `json:"needsBackupCount"`
	NoBackup       int       `json:"noBackupCount"`
	NeedBackupSize int64     `json:"needsBackupSize"`
	NoBackupSize   int64     `json:"noBackupSize"`
	Timestamp      time.Time `json:"timestamp"`
}

type BackupDebugEntry struct {
	FilePath            string  `json:"filePath"`
	Notes               string  `json:"notes"`
	PotentialMatchSizes []int64 `json:"potentialMatchSizes"`
}

type FileRecord struct {
	Timestamp           time.Time `json:"timestamp"`
	Filename            string    `json:"filename"`
	Extension           string    `json:"extension"`
	WorkingGroup        string    `json:"working_group"`
	Commission          string    `json:"commission"`
	Project             string    `json:"project"`
	StatErr             string    `json:"stat_error"`
	Size                int64     `json:"size"`
	WholePath           string    `json:"wholepath"`
	Notes               string    `json:"notes"`
	PotentialMatchSizes []int64   `json:"potentialMatchSizes"`
}

/**
takes data from the source and outputs an IndexRecord and a document ID (as a string)
arguments:
  - fromData: A pointer to a JsonFormat object containing the data to use
returns:
  - IndexRecord containing the parsed data and a timestamp
  - string containing a document ID derived from the timestamp
*/
func MakeIndexRecord(fromData *JsonFormat) (IndexRecord, string) {
	return IndexRecord{
		NeedBackup:     fromData.NeedBackup,
		NoBackup:       fromData.NoBackup,
		NeedBackupSize: fromData.NeedBackupSize,
		NoBackupSize:   fromData.NoBackupSize,
		Timestamp:      time.Now(),
	}, strconv.FormatInt(time.Now().UnixNano(), 10)
}

/**
return the default filename as expected from manual-media-backup

*/
func GetDefaultFilename() string {
	var basePath = os.Getenv("HOME")
	if basePath == "" {
		basePath = "/tmp"
	}
	return basePath + "/backup-estimate.json"
}

func GetDefaultListName() string {
	var basePath = os.Getenv("HOME")
	if basePath == "" {
		basePath = "/tmp"
	}
	return basePath + "/to-back-up.lst"
}

/**
establish a connection to ElasticSearch. Terminates if no connection can be established
*/
func connectToES(elasticUrlPtr *string) *elasticsearch.Client {
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

/**
Loads a json file and parses it as a single JsonFormat object
arguments:
 - fileNamePtr pointer to string of the file to open
returns:
 - a pointer to a JsonFormat object on success or nil on failure
 - nil on success or an error object on failure
*/
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

/**
reads in the given file in the background, passing out each line to a channel as it is read
*/
func ReadInListfile(fileNamePtr *string) (chan *BackupDebugEntry, error) {
	f, openErr := os.Open(*fileNamePtr)
	if openErr != nil {
		log.Printf("Could not open %s to read: %s", *fileNamePtr, openErr)
		return nil, openErr
	}

	//why a pointer? so we can pass nil to indicate end-of-stream.
	outputChan := make(chan *BackupDebugEntry, 10)

	go func() {
		defer f.Close()

		scanner := bufio.NewScanner(f) //scanner splits on newlines by default
		for scanner.Scan() {
			var entry BackupDebugEntry
			rawJson := scanner.Text()
			if rawJson == "" {
				continue
			}
			marshalErr := json.Unmarshal([]byte(rawJson), &entry)
			if marshalErr == nil {
				outputChan <- &entry
			} else {
				log.Printf("could not unmarshal line: %s", marshalErr)
			}
		}
		outputChan <- nil
	}()

	return outputChan, nil
}

func DoesIndexExist(esClient *elasticsearch.Client) (bool, error) {
	existsResponse, exErr := esClient.Indices.Exists([]string{"files-to-back-up"})

	if exErr != nil {
		log.Fatal("could not check for existence of old index: ", exErr)
	}
	defer existsResponse.Body.Close()
	io.Copy(ioutil.Discard, existsResponse.Body)
	switch existsResponse.StatusCode {
	case 200: //index exists
		return true, nil
	case 404: //index does not exist
		return false, nil
	default: //something else
		log.Fatal("DoesIndexExist got an unexpected response: ", existsResponse.StatusCode)
		return false, errors.New("should not get here")
	}
}

func RemoveOldList(esClient *elasticsearch.Client) {
	exists, existErr := DoesIndexExist(esClient)
	if existErr != nil {
		log.Fatal("could not check for index existence")
	}
	if exists {
		log.Printf("RemoveOldList: there is an existing index for file list, removing and re-creating")

		response, err := esClient.Indices.Delete([]string{"files-to-back-up"})
		if err != nil {
			log.Fatal("could not remove old index: ", err)
		}
		response.Body.Close()
	} else {
		log.Printf("RemoteOldList: no existing index, proceeding")
	}
}

var xtnsplitter = regexp.MustCompile("^(.*)\\.([^.]+)$")

func MakeFileRecord(entryPtr *BackupDebugEntry) *FileRecord {
	if entryPtr == nil {
		return nil
	}

	var rec FileRecord

	rec.WholePath = entryPtr.FilePath
	rec.Notes = entryPtr.Notes
	rec.PotentialMatchSizes = entryPtr.PotentialMatchSizes

	dirPart, filePart := path.Split(entryPtr.FilePath)
	stringParts := strings.Split(dirPart, "/")
	if len(stringParts) >= 7 {
		rec.WorkingGroup = stringParts[5]
		rec.Commission = stringParts[6]
		rec.Project = stringParts[7]
	}

	matches := xtnsplitter.FindStringSubmatch(filePart)
	if matches == nil {
		rec.Filename = filePart
	} else {
		rec.Filename = matches[1]
		rec.Extension = matches[2]
	}

	info, statErr := os.Stat(entryPtr.FilePath)
	if statErr == nil {
		rec.Size = info.Size()
		rec.Timestamp = info.ModTime()
	} else {
		rec.StatErr = statErr.Error()
		rec.Timestamp = time.Now()
	}
	return &rec
}

/**
sets up a loop to read from the given channel of filenames and pushes them into the index
no bulk is performed as yet because the number is expected to be relatively small
*/
func AddToIndex(esClient *elasticsearch.Client, fileListChan chan *BackupDebugEntry) int {
	ctr := 0
	for {
		entryPtr := <-fileListChan
		if entryPtr == nil {
			log.Print("AddToIndex got to end of data, returning")
			return ctr
		}
		ctr += 1
		ctx := context.Background()

		marshalledContent, _ := json.Marshal(MakeFileRecord(entryPtr))
		req := esapi.IndexRequest{
			Index:   "files-to-back-up",
			Body:    bytes.NewReader(marshalledContent),
			Refresh: "false",
		}
		_, err := req.Do(ctx, esClient)
		if err != nil {
			log.Printf("WARNING AddToIndex could not index %s due to %s", entryPtr.FilePath, err)
		}
	}
}

func main() {
	defaultFileName := GetDefaultFilename()
	fileNamePtr := flag.String("input-file", defaultFileName, "Name of the json file to import and upload")
	listNamePtr := flag.String("input-list", GetDefaultListName(), "Name of a text file to import filenames from")
	elasticUrlPtr := flag.String("elasticsearch", "http://localhost:9200", "URL to the Elasticsearch cluster")
	indexNamePtr := flag.String("index", "backup-estimate", "Name of the index to save data to")

	flag.Parse()
	esClient := connectToES(elasticUrlPtr)

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

	log.Printf("Reading file list in from %s", *listNamePtr)
	filesToBackUp, readErr := ReadInListfile(listNamePtr)
	if readErr != nil {
		log.Fatal("could not read in list file: ", readErr)
	}

	RemoveOldList(esClient)
	totalIndexed := AddToIndex(esClient, filesToBackUp)
	log.Printf("All %d files read in", totalIndexed)
}
