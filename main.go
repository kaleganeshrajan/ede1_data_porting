package main

import (
	"context"
	"ede_porting/headers"
	"ede_porting/models"
	sr "ede_porting/parsers"
	"ede_porting/utils"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	//cr "github.com/brkelkar/common_utils/configreader"
)

var (
	//cfg           cr.Config
	gcsFileAttr utils.GcsFile
	//awacsSubNames []string
	//projectID     string
	maxGoroutines int64
)

//BukectStruct parse data from pubsub
type BukectStruct struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Bucket  string    `json:"bucket"`
	Updated time.Time `json:"updated"`
	Size    string    `json:"size"`
}

func init() {
	//awacsSubNames = []string{"awacs-ede1-test-sub"}
	//projectID = "awacs-dev"
	maxGoroutines = 3
}

func main() {

	bucket := "awacs-mtd" //awacs-monthlydata //awacs-mtd
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("storage.NewClient: %v", err)
	}
	defer client.Close()

	// ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	// defer cancel()

	guard := make(chan struct{}, maxGoroutines)
	cm := make(chan *storage.ObjectAttrs)
	//day := 1
	//query := &storage.Query{Prefix: "UploadSSA/0" + strconv.Itoa(day)}
	go func() {
		it := client.Bucket(bucket).Objects(ctx, nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				//day = day + 1
				return
			}
			if err != nil {
				fmt.Printf("Bucket(%q).Objects: %v", bucket, err)
				continue
			}

			if strings.Contains(attrs.Name, "01-2021") {
				cm <- attrs
			}

		}
	}()

	for msg := range cm {
		guard <- struct{}{} // would block if guard channel is already filled
		go func(ctx context.Context) {
			//time.Sleep(100 * time.Millisecond)
			//fmt.Println(msg.Name)
			//log.Printf("Sending file Goroutines : %v\n", msg.Name)
			worker(ctx, msg.Name, bucket)
			<-guard
		}(ctx)
	}

	//ctx := context.Background()
	// //client, err := pubsub.NewClient(ctx, projectID)
	// if err != nil {
	// 	log.Printf("Error while recieving Message: %v", err)
	// }
	// defer client.Close()
	// var awacsSubscriptions []*pubsub.Subscription
	// for _, name := range awacsSubNames {
	// 	awacsSubscriptions = append(awacsSubscriptions, client.Subscription(name))
	// }
	// ctx, cancel := context.WithCancel(ctx)
	// defer cancel()
	// Create a channel to handle messages to as they come in.
	// cm := make(chan *pubsub.Message)
	// defer close(cm)
	// guard := make(chan struct{}, maxGoroutines)
	// log.Println("Starting go routines")
	// for _, sub := range awacsSubscriptions {
	// 	go func(sub *pubsub.Subscription) {
	// 		// Receive blocks until the context is cancelled or an error occurs.
	// 		err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
	// 			cm <- msg
	// 		})
	// 		if err != nil {
	// 			log.Printf("Subscription error := %v", err)
	// 		}
	// 	}(sub)
	// }
	// log.Println("Starting go Message reader")
	// for msg := range cm {
	// 	guard <- struct{}{} // would block if guard channel is already filled
	// 	go func(ctx context.Context, msg pubsub.Message) {
	// 		//msg.Ack()
	// 		time.Sleep(5 * time.Millisecond)
	// 		worker(ctx, msg)
	// 		<-guard
	// 	}(ctx, *msg)
	// }
}

func worker(ctx context.Context, filename string, bucketname string) {
	log.Printf("Receved file in worker : %v\n", filename)
	// if msg.Attributes["eventType"] == "OBJECT_DELETE" {
	// 	msg.Ack()
	// 	return
	// }
	//log.Printf("Start Message ID: %v ObjectCreation: %v ObjectID: %v", msg.ID, msg.Attributes["objectGeneration"], msg.Attributes["objectId"])
	//defer ackMessgae(msg)
	//var bucketDetails BukectStruct
	//json.Unmarshal(msg.Data, &bucketDetails)
	var e models.GCSEvent
	e.Bucket = bucketname
	e.Name = filename
	e.Updated = time.Now()
	e.Size = "10"

	g := *gcsFileAttr.HandleGCSEvent(ctx, e)
	if !g.GcsClient.GetLastStatus() {
		return
	}

	// g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-test")
	// return
	var ef utils.ErrorFileDetail
	var r io.Reader
	var reader *csv.Reader
	if !strings.Contains(strings.ToUpper(g.FileName), "STANDARD V4") || !strings.Contains(strings.ToUpper(g.FileName), "STANDARD EXCEL") {
		r = g.GcsClient.GetReader()
		reader = csv.NewReader(r)

		if reader == nil {
			ef.ErrorFileDetails(g.FilePath, "error while getting reader", headers.Error_File_details, g)
			log.Println("error while getting reader")
			return
		}
	}

	switch {
	case strings.Contains(strings.ToUpper(g.FileName), "AWACS PATCH"):
		err := sr.StockandSalesParser(g, reader)
		if err != nil {
			ef.ErrorFileDetails(g.FilePath, err.Error(), headers.Error_File_details, g)
			log.Println(err)
		}
	case strings.Contains(strings.ToUpper(g.FileName), "CSV"):
		//log.Printf("File Start :-%v\n", g.FileName)
		err := sr.StockandSalesCSVParser(g, reader)
		if err != nil {
			ef.ErrorFileDetails(g.FilePath, err.Error(), headers.Error_File_details, g)
			log.Println(err)
		}
	case strings.Contains(strings.ToUpper(g.FileName), "STANDARD V4"), strings.Contains(strings.ToUpper(g.FileName), "STANDARD EXCEL"):

		script := "./file_convert/ede_xls_dbf_to_csv.py"
		fileName := "gs://" + g.FilePath
		temp := strings.Split(g.FilePath, "/")

		tUnix := strconv.Itoa(int(time.Now().Unix()))
		outPutFile := "/tmp/" + temp[len(temp)-2] + "_" + temp[len(temp)-1] + "_" + tUnix + ".csv"
		//log.Println(script, "-p", fileName, "-d", outPutFile)
		cmd := exec.Command(script, "-p", fileName, "-d", outPutFile)

		err := cmd.Run()
		if err != nil {
			ef.ErrorFileDetails(g.FilePath, "Error while running command : "+err.Error(), headers.Error_File_details, g)
			log.Printf("Error while running command : %v\n", err.Error())
			return
		}
		fd, err := os.Open(outPutFile)
		//os.Remove(outPutFile)
		defer os.Remove(outPutFile)
		if err != nil {
			ef.ErrorFileDetails(g.FilePath, "Error while open Excel file : "+err.Error(), headers.Error_File_details, g)
			log.Printf("Error while open Excel file : %v\n", err.Error())
			return
		}

		readerin := csv.NewReader(fd)
		if readerin == nil {
			ef.ErrorFileDetails(g.FilePath, "error while getting reader", headers.Error_File_details, g)
			log.Println("error while getting reader")
			return
		}

		if strings.Contains(strings.ToUpper(g.FileName), "SALE_DTL") {
			err := sr.StockandSalesSale(g, readerin)
			if err != nil {
				ef.ErrorFileDetails(g.FilePath, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return
			}
		} else if strings.Contains(strings.ToUpper(g.FileName), ".XLS") || strings.Contains(strings.ToUpper(g.FileName), ".XLSX") {
			err := sr.StockandSalesDetails(g, readerin)
			if err != nil {
				ef.ErrorFileDetails(g.FilePath, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return
			}
		} else {
			err := sr.StockandSalesDits(g, readerin)
			if err != nil {
				ef.ErrorFileDetails(g.FilePath, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return
			}
		}
	case strings.Contains(strings.ToUpper(g.FileName), "STANDARD V5"):
		if strings.Contains(strings.ToUpper(g.FileName), "SALE_DTL") {
			err := sr.StockandSalesSale(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FilePath, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return
			}
		} else {
			err := sr.StockandSalesDits(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FilePath, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return
			}
		}
	}

}
