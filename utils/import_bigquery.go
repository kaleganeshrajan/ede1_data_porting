package utils

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
)

//ImportCSVFromFile import data to big query
func ImporttoBigquery(projectID, datasetID, tableID, filename string) (err error) {
	log.Println("File import to bigquery start")
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
	log.Println("File import to bigquery end")
	return nil
}
