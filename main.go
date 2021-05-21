package main

import (
	"bufio"
	"context"
	"ede_porting/models"
	sr "ede_porting/parsers"
	"ede_porting/utils"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	cr "github.com/brkelkar/common_utils/configreader"
)

var (
	cfg           cr.Config
	gcsFileAttr   utils.GcsFile
	awacsSubNames []string
	projectID     string
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
	awacsSubNames = []string{"awacs-ede1-test-sub"}
	projectID = "awacs-dev"
	maxGoroutines = 15
}

func main() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Printf("Error while recieving Message: %v", err)
	}
	defer client.Close()
	var awacsSubscriptions []*pubsub.Subscription

	for _, name := range awacsSubNames {
		awacsSubscriptions = append(awacsSubscriptions, client.Subscription(name))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create a channel to handle messages to as they come in.
	cm := make(chan *pubsub.Message)

	defer close(cm)
	guard := make(chan struct{}, maxGoroutines)
	log.Println("Starting go routines")
	for _, sub := range awacsSubscriptions {
		go func(sub *pubsub.Subscription) {
			// Receive blocks until the context is cancelled or an error occurs.
			err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				cm <- msg
			})
			if err != nil {
				log.Printf("Subscription error := %v", err)
			}
		}(sub)
	}
	log.Println("Starting go Message reader")
	for msg := range cm {
		guard <- struct{}{} // would block if guard channel is already filled
		go func(ctx context.Context, msg pubsub.Message) {
			//msg.Ack()
			time.Sleep(5 * time.Millisecond)
			worker(ctx, msg)
			<-guard
		}(ctx, *msg)
	}
}

func worker(ctx context.Context, msg pubsub.Message) {
	if msg.Attributes["eventType"] == "OBJECT_DELETE" {
		msg.Ack()
		return
	}
	log.Printf("Start Message ID: %v ObjectCreation: %v ObjectID: %v", msg.ID, msg.Attributes["objectGeneration"], msg.Attributes["objectId"])
	//defer ackMessgae(msg)
	var bucketDetails BukectStruct
	json.Unmarshal(msg.Data, &bucketDetails)
	var e models.GCSEvent
	e.Bucket = bucketDetails.Bucket
	e.Name = bucketDetails.Name
	e.Updated = bucketDetails.Updated
	e.Size = bucketDetails.Size

	var mu sync.Mutex
	mu.Lock()
	g := *gcsFileAttr.HandleGCSEvent(ctx, e)
	if !g.GcsClient.GetLastStatus() {
		return
	}
	mu.Unlock()
	switch {
	case strings.Contains(strings.ToUpper(g.FileName), "AWACS PATCH"):
		msg.Ack()
		ackMessgae(msg)
		err := sr.StockandSalesParser(g, cfg)
		if err != nil {
			log.Println(err)
		}
	case strings.Contains(strings.ToUpper(g.FileName), "CSV"):
		msg.Ack()
		ackMessgae(msg)
		err := sr.StockandSalesCSVParser(g, cfg)
		if err != nil {
			log.Println(err)
		}
	case strings.Contains(strings.ToUpper(g.FileName), "STANDARD V4"), strings.Contains(strings.ToUpper(g.FileName), "STANDARD EXCEL"):
		script := "./file_convert/ede_xls_dbf_to_csv.py"
		fileName := "gs://" + g.FilePath
		temp := strings.Split(g.FilePath, "/")

		//log.Printf("Message Id : %v Object Generation : %v Object Id : %v\n", msg.ID, msg.Attributes["objectGeneration"], msg.Attributes["objectId"])

		outPutFile := "gs://awacs-ede1-test/" + temp[len(temp)-2] + "_" + temp[len(temp)-1] + ".csv"
		log.Println(script, "-p", fileName, "-d", outPutFile)
		cmd := exec.Command(script, "-p", fileName, "-d", outPutFile)

		cmd.Run()
		fd, err := os.Open(outPutFile)
		defer os.Remove(outPutFile)
		if err != nil {
			log.Printf("Error while open Excel file : %v\n", err)
			return
		}

		reader := bufio.NewReader(fd)
		msg.Ack()
		ackMessgae(msg)
		if strings.Contains(strings.ToUpper(g.FileName), "SALE_DTL") {
			err := sr.StockandSalesSale(g, cfg, reader)
			if err != nil {
				log.Println(err)
				return
			}
		} else if strings.Contains(strings.ToUpper(g.FileName), ".XLS") || strings.Contains(strings.ToUpper(g.FileName), ".XLSX") {
			err := sr.StockandSalesDetails(g, cfg, reader)
			if err != nil {
				log.Println(err)
				return
			}
		} else {
			err := sr.StockandSalesDits(g, cfg, reader)
			if err != nil {
				log.Println(err)
				return
			}
		}
	case strings.Contains(strings.ToUpper(g.FileName), "STANDARD V5"):
		r := g.GcsClient.GetReader()
		reader := bufio.NewReader(r)

		msg.Ack()
		ackMessgae(msg)
		if strings.Contains(strings.ToUpper(g.FileName), "SALE_DTL") {
			err := sr.StockandSalesSale(g, cfg, reader)
			if err != nil {
				log.Println(err)
			}
		} else {
			err := sr.StockandSalesDits(g, cfg, reader)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func ackMessgae(msg pubsub.Message) {
	log.Printf("Ack Message ID: %v ObjectCreation: %v ObjectID: %v\n", msg.ID, msg.Attributes["objectGeneration"], msg.Attributes["objectId"])
}
