package parsers

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"ede_porting/headers"
	"ede_porting/models"
	"ede_porting/utils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var (
	gcsFileAttr utils.GcsFile
)

//BukectStruct parse data from pubsub
type BukectStruct struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Bucket  string    `json:"bucket"`
	Updated time.Time `json:"updated"`
	Size    string    `json:"size"`
}

func Worker(msg models.PubSubMessage) error {
	var bucketDetails BukectStruct
	//var fs utils.FileStore
	var ef utils.ErrorFileDetail
	var r io.Reader
	var reader *bufio.Reader

	json.Unmarshal(msg.Message.Data, &bucketDetails)
	//log.Printf("bucketDetails.Name :-%v\n", bucketDetails.Name)
	var e models.GCSEvent
	e.Bucket = bucketDetails.Bucket
	e.Name = bucketDetails.Name
	e.Updated = bucketDetails.Updated
	e.Size = bucketDetails.Size

	ctx := context.Background()
	g := *gcsFileAttr.HandleGCSEvent(ctx, e)
	if !g.GcsClient.GetLastStatus() {
		return nil
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
				return err
			}
			e := os.Remove(fileName)
			if e != nil {
				log.Fatal(e)
			}
		}
		return nil
	}

	if !strings.Contains(strings.ToUpper(g.FileName), "STANDARD V4") || !strings.Contains(strings.ToUpper(g.FileName), "STANDARDV4") || !strings.Contains(strings.ToUpper(g.FileName), "STANDARD EXCEL") || !strings.Contains(strings.ToUpper(g.FileName), "STANDARDEXCEL") {
		r = g.GcsClient.GetReader()
		reader = bufio.NewReader(r)

		if reader == nil {
			ef.ErrorFileDetails(g.FileName, "error while getting reader", headers.Error_File_details, g)
			log.Println("error while getting reader")
			return nil
		}
	}

	ParsersCall(g, reader)

	return nil
}

func ParsersCall(g utils.GcsFile, reader *bufio.Reader) (err error) {
	var ef utils.ErrorFileDetail
	switch {
	case strings.Contains(strings.ToUpper(g.FileName), "AWACS PATCH"), strings.Contains(strings.ToUpper(g.FileName), "PATCH2010"):
		err := StockandSalesParser(g, reader)
		if err != nil {
			ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
			log.Println(err)
		}
	case strings.Contains(strings.ToUpper(g.FileName), "CSV"):
		//log.Printf("File Start :-%v\n", g.FileName)
		err := StockandSalesCSVParser(g, reader)
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
			err := StockandSalesSale(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		} else if strings.Contains(strings.ToUpper(g.FileName), ".XLS") || strings.Contains(strings.ToUpper(g.FileName), ".XLSX") {
			err := StockandSalesDetails(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		} else {
			err := StockandSalesDits(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		}
	case strings.Contains(strings.ToUpper(g.FileName), "STANDARD V5"):
		if strings.Contains(strings.ToUpper(g.FileName), "SALE_DTL") {
			err := StockandSalesSale(g, reader)
			if err != nil {
				ef.ErrorFileDetails(g.FileName, err.Error(), headers.Error_File_details, g)
				log.Println(err)
				return err
			}
		} else {
			err := StockandSalesDits(g, reader)
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
