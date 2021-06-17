package parsers

import (
	"bufio"
	hd "ede_porting/headers"
	md "ede_porting/models"
	ut "ede_porting/utils"
	"errors"
	"io"
	"log"
	"strings"
	"time"
)

func StockandSalesDits(g ut.GcsFile, reader *bufio.Reader) (err error) {
	startTime := time.Now()
	//log.Printf("Starting file parse: %v", g.FilePath)

	var recordsDist md.RecordDist

	var fd ut.FileDetail

	flag := 1
	seperator := "\x10"
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
			if len(lineSlice) <= 3 {
				seperator = ";"
				lineSlice = strings.Split(line, seperator)
			}
		}
		if flag == 1 {
			flag = 0
		} else {
			if len(lineSlice) == 6 {
				recordsDist = assignItems(lineSlice)
				if strings.Contains(g.BucketName, "MTD") {
					recordsDist.Duration = hd.DurationMTD
				} else {
					recordsDist.Duration = hd.DurationMonthly
				}
				recordsDist.FilePath = strings.TrimSpace(g.FilePath)

			} else {
				return errors.New("file is not correct format")
			}
		}
	}

	if recordsDist.DistributorCode != "" {
		testinter := recordsDist
		err = ut.GenerateJsonFile(testinter, hd.Stock_and_Sales_Dist)
		if err != nil {
			return err
		}

		fd.FileDetails(g.FilePath, recordsDist.DistributorCode, 1, 0, 0, int64(time.Since(startTime)/1000000), hd.File_details)

		g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
		//log.Printf("File parsing done: %v", g.FilePath)

		g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
		//g.LogFileDetails(true)
	} else {
		return errors.New("file is empty")
	}

	return err
}

func assignItems(lineSlice []string) (recordsDist md.RecordDist) {
	var cm md.Common
	var err error
	recordsDist.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	recordsDist.CityName = strings.TrimSpace(lineSlice[hd.CityName])
	recordsDist.DistName = strings.TrimSpace(lineSlice[hd.DistName])
	recordsDist.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockist])
	recordsDist.StateName = strings.TrimSpace(lineSlice[hd.StateName])

	cm.FromDate, err = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.DFromDate]))
	if err != nil ||cm.FromDate==nil{
		log.Printf("stockandsales_dist From Date Error: %v : %v", err, lineSlice[hd.DFromDate])
	} else {
		recordsDist.FromDate = cm.FromDate.Format("2006-01-02")
	}
	cm.ToDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.DToDate]))
	if err != nil||cm.ToDate==nil {
		log.Printf("stockandsales_dist To Date Error: %v : %v", err, lineSlice[hd.DToDate])
	} else {
		recordsDist.ToDate = cm.ToDate.Format("2006-01-02")
	}
	return recordsDist
}
