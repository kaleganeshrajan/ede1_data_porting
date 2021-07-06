package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"ede_porting/headers"
	"ede_porting/models"
	sr "ede_porting/parsers"
	"ede_porting/utils"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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
	maxGoroutines = 1
}

func main() {
	bucket := "awacs-test" //awacs-monthlydata //awacs-mtd
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

			// if strings.Contains(attrs.Name, "06-2021") {
			// 	cm <- attrs
			// }
			cm <- attrs
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
	var r io.Reader
	var reader *bufio.Reader
	var ef utils.ErrorFileDetail
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

	ctx1 := context.Background()
	g := *gcsFileAttr.HandleGCSEvent(ctx1, e)
	if !g.GcsClient.GetLastStatus() {
		return
	}

	// err := fs.StoreFile(g.FilePath, headers.FileNotPorted, g.BucketName, headers.File_Store)
	// if err != nil {
	// 	log.Printf("error while storing file in file_store table : %v\n", err)
	// 	return nil
	// }

	if strings.Contains(g.FileName, ".zip") {
		files, err := Unzip(g, headers.ZipOutFile)
		if err != nil {
			log.Fatal(err)
		}

		for _, fileName := range files {
			file, er := os.Open(fileName)
			if er != nil {
				fmt.Println(er)
			}
			reader = bufio.NewReader(file)
			if strings.Contains(strings.ToUpper(fileName), "STANDARD V4") || strings.Contains(strings.ToUpper(fileName), "STANDARD EXCEL") {
				g.FilePath = fileName
			}
			err = ParsersCall(g, reader)
			if err != nil {
				return
			}
			e := os.Remove(fileName)
			if e != nil {
				log.Fatal(e)
			}
		}
		return
	}

	if !strings.Contains(strings.ToUpper(g.FileName), "STANDARD V4") || !strings.Contains(strings.ToUpper(g.FileName), "STANDARDV4") || !strings.Contains(strings.ToUpper(g.FileName), "STANDARD EXCEL") || !strings.Contains(strings.ToUpper(g.FileName), "STANDARDEXCEL") {
		r = g.GcsClient.GetReader()
		reader = bufio.NewReader(r)

		if reader == nil {
			ef.ErrorFileDetails(g.FileName, "error while getting reader", headers.Error_File_details, g)
			log.Println("error while getting reader")
			return
		}
	}

	ParsersCall(g, reader)

}

func ParsersCall(g utils.GcsFile, reader *bufio.Reader) (err error) {
	var ef utils.ErrorFileDetail
	switch {
	case strings.Contains(strings.ToUpper(g.FileName), "AWACS PATCH"), strings.Contains(strings.ToUpper(g.FileName), "PATCH2010"):
		err := sr.StockandSalesParser(g, reader)
		if err != nil {
			ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
			log.Println(err)
		}
	case strings.Contains(strings.ToUpper(g.FileName), "CSV"):
		//log.Printf("File Start :-%v\n", g.FileName)
		err := sr.StockandSalesCSVParser(g, reader)
		if err != nil {
			ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
			log.Println(err)
		}
	case strings.Contains(strings.ToUpper(g.FileName), "STANDARD V4"), strings.Contains(strings.ToUpper(g.FileName), "STANDARDV4"), strings.Contains(strings.ToUpper(g.FileName), "STANDARD EXCEL"), strings.Contains(strings.ToUpper(g.FileName), "STANDARDEXCEL"):
		script := "./file_convert/ede_xls_dbf_to_csv.py"
		fileName := "gs://" + g.FilePath
		temp := strings.Split(g.FilePath, "/")

		outPutFile := headers.Filename + temp[len(temp)-2] + "_" + temp[len(temp)-1] + ".csv"
		//log.Println(script, "-p", fileName, "-d", outPutFile)
		cmd := exec.Command(script, "-p", fileName, "-d", outPutFile)

		err := cmd.Run()
		if err != nil {
			ef.ErrorFileDetails(g.FileName, "Error while running command : "+err.Error(), headers.Error_File_details, g)
			log.Printf("Error while running command : %v\n", err.Error())
			return err
		}
		fd, err := os.Open(outPutFile)
		defer os.Remove(outPutFile)
		if err != nil {
			ef.ErrorFileDetails(g.FileName, "Error while open Excel file", headers.Error_File_details, g)
			log.Printf("Error while open Excel file : %v\n", err)
			return err
		}

		reader = bufio.NewReader(fd)
		if reader == nil {
			ef.ErrorFileDetails(g.FileName, "error while getting reader", headers.Error_File_details, g)
			log.Println("error while getting reader")
			return nil
		}

		if strings.Contains(strings.ToUpper(g.FileName), "SALE_DTL") {
			err := sr.StockandSalesSale(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		} else if strings.Contains(strings.ToUpper(g.FileName), ".XLS") || strings.Contains(strings.ToUpper(g.FileName), ".XLSX") {
			err := sr.StockandSalesDetails(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		} else {
			err := sr.StockandSalesDits(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		}
	case strings.Contains(strings.ToUpper(g.FileName), "STANDARD V5"):
		if strings.Contains(strings.ToUpper(g.FileName), "SALE_DTL") {
			err := sr.StockandSalesSale(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		} else {
			err := sr.StockandSalesDits(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		}
	}

	return nil
}

// Unzip will decompress a zip archive, moving all files and folders
// within the zip file (parameter 1) to an output directory (parameter 2).
func Unzip(g utils.GcsFile, dest string) ([]string, error) {

	buf := &bytes.Buffer{}
	buf.ReadFrom(g.GcsClient.GetReader())

	// retrieve a byte slice from bytes.Buffer
	data := buf.Bytes()
	var filenames []string

	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return filenames, err
	}

	for _, f := range r.File {
		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)

		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return filenames, fmt.Errorf("%s: illegal file path", fpath)
		}

		filenames = append(filenames, fpath)

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return filenames, err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())

		if err != nil {
			return filenames, err
		}

		rc, err := f.Open()
		if err != nil {
			return filenames, err
		}

		_, err = io.Copy(outFile, rc)

		// Close the file without defer to close before next iteration of loop
		outFile.Close()
		rc.Close()

		if err != nil {
			return filenames, err
		}
	}
	return filenames, nil
}
