package parsers

import (
	"bufio"
	hd "ede_porting/headers"
	md "ede_porting/models"
	ut "ede_porting/utils"
	"io"
	"log"
	"strings"
	"time"

	cr "github.com/brkelkar/common_utils/configreader"
)

func StockandSalesDits(g ut.GcsFile, cfg cr.Config, reader *bufio.Reader) (err error) {
	startTime := time.Now()
	log.Printf("Starting file parse: %v", g.FilePath)

	if reader == nil {
		log.Println("error while getting reader")
		return
	}
	var recordsDist md.RecordDist
	recordsDist.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	var fd ut.FileDetail

	flag := 1
	seperator := ";"
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		if len(line) <= 2 {
			break
		}

		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, seperator)
		if len(lineSlice) <= 3 {
			seperator = "|"
			lineSlice = strings.Split(line, seperator)
		}
		if flag == 1 {
			flag = 0
		} else {
			if len(lineSlice) < 6 {
				log.Println("File is not correct format")
				return nil
			}
			recordsDist := assignItems(lineSlice)
			recordsDist.Key = strings.TrimSpace(g.FileKey)
			if strings.Contains(g.BucketName, "MTD") {
				recordsDist.Duration = hd.DurationMTD
			} else {
				recordsDist.Duration = hd.DurationMonthly
			}
			recordsDist.FilePath = strings.TrimSpace(g.FilePath)
		}
	}

	testinter := recordsDist
	err = ut.GenerateJsonFile(testinter, hd.Stock_and_Sales_Dist)
	if err != nil {
		return err
	}

	fd.FileDetails(g.FilePath, recordsDist.DistributorCode, 1, 0, 0, int64(time.Since(startTime)/1000000), hd.File_details)

	g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
	log.Printf("File parsing done: %v", g.FilePath)

	g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
	g.LogFileDetails(true)

	return err
}

func assignItems(lineSlice []string) (recordsDist md.RecordDist) {
	var cm md.Common
	recordsDist.CityName = strings.TrimSpace(lineSlice[hd.CityName])
	recordsDist.DistName = strings.TrimSpace(lineSlice[hd.DistName])
	recordsDist.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockist])
	recordsDist.StateName = strings.TrimSpace(lineSlice[hd.StateName])
	cm.FromDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.DFromDate]))
	cm.ToDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.DToDate]))
	recordsDist.FromDate = cm.FromDate.Format("2006-01-02")
	recordsDist.ToDate = cm.ToDate.Format("2006-01-02")
	return recordsDist
}
