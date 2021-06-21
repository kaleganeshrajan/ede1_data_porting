package utils

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	hd "ede_porting/headers"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
)

//GenerateJsonFile generate json file and insert to bigquery
func GenerateJsonFile(Rrecords interface{}, tableName string) (err error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, hd.ProjectID)
	if err != nil {
		log.Printf("bigquery.NewClient: %v", err)
		return err
	}
	defer client.Close()

	inserter := client.Dataset(hd.DatasetID).Table(tableName).Inserter()

	if err := inserter.Put(ctx, Rrecords); err != nil {
		log.Println(err)
		return err
	}
	return nil

	// file, err := json.Marshal(Rrecords)
	// if err != nil {
	// 	panic(err)
	// }
	// Filename := hd.Filename + uuid.New().String() + ".json"

	// err = ioutil.WriteFile(Filename, file, 0644)
	// if err != nil {
	// 	log.Printf("Error while creating Json file: %v", err)
	// 	log.Printf("Error while creating Json file: %v", err)
	// 	return err
	// }

	// err = ImporttoBigquery(hd.ProjectID, hd.DatasetID, tableName, Filename)
	// if err != nil {
	// 	log.Printf("Error while importing to bigquery: %v", err)
	// 	return err
	// }

	// return nil
}

//GenerateJsonFile generate json file and insert to bigquery
func GenerateJsonFile1(Rrecords interface{}, tableName string) (err error) {
	file, err := json.Marshal(Rrecords)
	if err != nil {
		panic(err)
	}
	Filename := hd.Filename + uuid.New().String() + ".json"

	err = ioutil.WriteFile(Filename, file, 0644)
	if err != nil {
		log.Printf("Error while creating Json file: %v", err)
		return err
	}

	err = ImporttoBigquery(hd.ProjectID, hd.DatasetID, tableName, Filename)
	if err != nil {
		log.Printf("Error while importing to bigquery: %v", err)
		return err
	}

	return nil
}

//ImportCSVFromFile import data to big query
func ImporttoBigquery(projectID, datasetID, tableID, filename string) (err error) {
	//log.Printf("File import to bigquery start, TableName := %v", tableID)
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Printf("bigquery.NewClient: %v", err)
		return err
	}
	defer client.Close()
	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	source := bigquery.NewReaderSource(f)
	source.SourceFormat = bigquery.JSON

	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteAppend
	job, err := loader.Run(ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	if err := status.Err(); err != nil {
		log.Println(err)
		return err
	}
	os.Remove(filename)
	//log.Printf("File import to bigquery end, TableName := %v", tableID)
	return nil
}
