package parsers

import (
	"bufio"
	hd "ede1_data_porting/headers"
	md "ede1_data_porting/models"
	ut "ede1_data_porting/utils"
	"io"
	"log"
	"strings"
	"time"

	cr "github.com/brkelkar/common_utils/configreader"
)

func StockandSalesDits(g ut.GcsFile, cfg cr.Config) (err error) {
	startTime := time.Now()
	log.Printf("Starting file parse: %v", g.FilePath)

	r := g.GcsClient.GetReader()
	reader := bufio.NewReader(r)
	if reader == nil {
		log.Println("error while getting reader")
		return
	}
	var recordsDist md.RecordDist

	flag := 1

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		if len(line) <= 2 {
			break
		}

		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, ";")

		if flag == 1 {
			flag = 0
		} else {
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
