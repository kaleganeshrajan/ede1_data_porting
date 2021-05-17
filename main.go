package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"

	"ede1_data_porting/models"
	sr "ede1_data_porting/services"
	"ede1_data_porting/utils"

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
			time.Sleep(5 * time.Millisecond)
			worker(ctx, msg)
			<-guard
		}(ctx, *msg)
	}
}

func worker(ctx context.Context, msg pubsub.Message) {
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
		msg.Ack()
		return
	}
	mu.Unlock()

	switch {
	case strings.Contains(strings.ToUpper(g.FileName), "AWACS PATCH"):
		msg.Ack()
		err := sr.StockandSalesParser(g, cfg)
		if err == nil {
			msg.Ack()
		}
	case strings.Contains(strings.ToUpper(g.FileName), "CSV"):
		err := sr.StockandSalesCSVParser(g, cfg)
		if err == nil {
			msg.Ack()
		}
	case strings.Contains(strings.ToUpper(g.FileName), "STANDARD"):
		cmd := exec.Command("main.py -p gs://balatestawacs/SampleFiles/AIOCD0923/AIOCD0923_02_2021_511b9d2d-76c3-4e4e-a2a4-35840fc612ce.xls --dpath D:/RDP/pqr.csv")
		out, err := cmd.Output()

		if err != nil {
			println(err.Error())
			return
		}

		fmt.Println(string(out))
	}
}
