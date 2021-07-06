package parsers

import (
	"bufio"
	hd "ede_porting/headers"
	md "ede_porting/models"
	"ede_porting/utils"
	"errors"
	"io"
	"log"
	"strings"
	"time"
)

func StockandSalesDits(g utils.GcsFile, reader *bufio.Reader) (err error) {

	startTime := time.Now().In(utils.ConvertUTCtoIST())
	//log.Printf("Starting file parse: %v", g.FileName)

	var recordsDist md.RecordDist

	var fd utils.FileDetail

	flag := 1
	seperator := "\x10"
	newLine := byte('\n')
	for {
		line, err := reader.ReadString(newLine)

		if len(line) > 30000  {
			reader = bufio.NewReader(strings.NewReader(line))
			newLine = '\r'
			continue
		} 

		if len(line) <= 2 && err == nil {
			continue
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
				recordsDist = assignItems(lineSlice, g)
				//recordsDist.Key = strings.TrimSpace(g.FileKey)
				if strings.Contains(strings.ToUpper(g.BucketName), "MTD") {
					recordsDist.Duration = hd.DurationMTD
				} else {
					recordsDist.Duration = hd.DurationMonthly
				}
				recordsDist.FilePath = strings.TrimSpace(g.FileName)

			} else {
				return errors.New("file is not correct format : " + line)
			}
		}
		if err != nil && err == io.EOF {
			break
		}
	}

	if recordsDist.DistributorCode != "" {
		testinter := recordsDist
		err = utils.InserttoBigquery(testinter, hd.Stock_and_Sales_Dist)
		if err != nil {
			return err
		}

		fd.FileDetails(g.FileName, recordsDist.DistributorCode, 1, 0, 0, int64(time.Since(startTime)/1000000),
			recordsDist.FromDate, recordsDist.ToDate, hd.File_details)

		g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
		//log.Printf("File parsing done: %v", g.FileName)

		g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
		//g.LogFileDetails(true)
	} else {
		return errors.New("file is empty")
	}

	return err
}

func assignItems(lineSlice []string, g utils.GcsFile) (recordsDist md.RecordDist) {
	var err error
	recordsDist.CreationDatetime = time.Now().In(utils.ConvertUTCtoIST()).Format("2006-01-02 15:04:05")
	recordsDist.CityName = strings.TrimSpace(lineSlice[hd.CityName])
	recordsDist.DistName = strings.TrimSpace(lineSlice[hd.DistName])
	recordsDist.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockist])
	recordsDist.StateName = strings.TrimSpace(lineSlice[hd.StateName])

	cm.FromDate, err = utils.ConvertDate(strings.TrimSpace(lineSlice[hd.DFromDate]))
	if err != nil || cm.FromDate == nil {
		log.Printf("stockandsales_dist From Date Error: %v : %v : %v\n", err, lineSlice[hd.DFromDate], g.FileName)
	} else {
		recordsDist.FromDate = cm.FromDate.Format("2006-01-02")
	}
	cm.ToDate, _ = utils.ConvertDate(strings.TrimSpace(lineSlice[hd.DToDate]))
	if err != nil || cm.ToDate == nil {
		log.Printf("stockandsales_dist To Date Error: %v : %v : %v\n", err, lineSlice[hd.DToDate], g.FileName)
	} else {
		recordsDist.ToDate = cm.ToDate.Format("2006-01-02")
	}
	return recordsDist
}
