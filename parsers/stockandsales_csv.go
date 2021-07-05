package parsers

import (
	"bufio"
	"ede_porting/headers"
	"ede_porting/models"
	"ede_porting/utils"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

//StockandSalesParser parse stock and sales with PTR and without PTR
func StockandSalesCSVParser(g utils.GcsFile, reader *bufio.Reader) (err error) {

	startTime := time.Now().In(utils.ConvertUTCtoIST())
	//log.Printf("Starting file parse: %v", g.FileName)

	var records models.Record
	cMap := make(map[string]models.Company)
	var fd utils.FileDetail

	records.FilePath = g.FileName
	if strings.Contains(strings.ToUpper(g.FileName), "CSV 1.0") {
		records.FileType = strconv.Itoa(headers.CSV_1_0)
	} else {
		records.FileType = strconv.Itoa(headers.CSV_1_1)
	}
	records.CreationDatetime = time.Now().In(utils.ConvertUTCtoIST()).Format("2006-01-02 15:04:05")
	if strings.Contains(strings.ToUpper(g.BucketName), "MTD") {
		records.Duration = headers.DurationMTD
	} else {
		records.Duration = headers.DurationMonthly
	}
	SS_count := 0

	newLine := byte('\n')
	for {
		line, err := reader.ReadString(newLine)

		if err != nil && len(line) > 1000 {
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
		lineSlice := strings.Split(line, ",")

		lineSlice[0] = utils.Removespacialcharactor(lineSlice[0])

		switch lineSlice[0] {
		case "H":
			records.DistributorCode = strings.TrimSpace(lineSlice[headers.Stockist_Code])

			cm.FromDate, err = utils.ConvertDate(strings.TrimSpace(lineSlice[headers.From_Date]))
			if err != nil || cm.FromDate == nil {
				log.Printf("stockandsales_csv From Date Error: %v : %v : %v\n", err, lineSlice[headers.From_Date], g.FileName)
			} else {
				records.FromDate = cm.FromDate.Format("2006-01-02")
			}
			cm.ToDate, err = utils.ConvertDate(strings.TrimSpace(lineSlice[headers.To_Date]))
			if err != nil || cm.ToDate == nil {
				log.Printf("stockandsales_csv To Date Error: %v : %v : %v\n", err, lineSlice[headers.To_Date], g.FileName)
			} else {
				records.ToDate = cm.ToDate.Format("2006-01-02")
			}
		case "T":
			SS_count = SS_count + 1
			tempItem := AssignItem(lineSlice)

			if _, ok := cMap[strings.TrimSpace(lineSlice[headers.Company_code])]; !ok {
				tempCompany := assignCompanySS(lineSlice)
				cMap[strings.TrimSpace(lineSlice[headers.Company_code])] = tempCompany
			}
			t := cMap[strings.TrimSpace(lineSlice[headers.Company_code])]
			t.Items = append(t.Items, tempItem)
			cMap[strings.TrimSpace(lineSlice[headers.Company_code])] = t

		}
		if err != nil && err == io.EOF {
			break
		}
	}

	var testinter interface{}
	if len(cMap) > 0 {
		for _, val := range cMap {
			records.Companies = append(records.Companies, val)
		}
		testinter = records
		err = utils.InserttoBigquery(testinter, headers.Stock_and_Sales)
		if err != nil {
			return err
		}

		fd.FileDetails(g.FileName, records.DistributorCode, SS_count, 0,
			0, int64(time.Since(startTime)/1000000), records.FromDate, records.ToDate, headers.File_details)

		g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
		//log.Printf("File parsing done: %v", g.FileName)

		g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
	} else {
		return errors.New("file is empty")
	}

	//g.LogFileDetails(true)
	return nil
}

func AssignItem(lineSlice []string) (tempItem models.Item) {
	PTRLength := 0
	tempItem.UniformPdtCode = strings.TrimSpace(lineSlice[headers.Csv_Uniform_Pdt_Code])
	tempItem.ItemCode = strings.TrimSpace(lineSlice[headers.Csv_Stkt_Product_Code])
	tempItem.ItemName = strings.TrimSpace(lineSlice[headers.Csv_Product_Name])
	SearchString, err := utils.ReplaceSpacialCharactor(strings.TrimSpace(lineSlice[headers.Csv_Product_Name]))
	if err != nil {
		log.Printf("Error while replacing spacail charactor : %v\n", err)
	} else {
		tempItem.SearchString = SearchString
	}
	tempItem.Pack = strings.TrimSpace(lineSlice[headers.Csv_Pack])
	if len(lineSlice) >= 16 {
		tempItem.PTR, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_PTR]), 64)
		PTRLength = 1
	}
	tempItem.OpeningStock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Opening_Qty+PTRLength]), 64)
	tempItem.PurchasesReciept, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Receipts_Qty+PTRLength]), 64)
	tempItem.SalesQty, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Sales_Qty+PTRLength]), 64)
	tempItem.SalesReturn, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Sales_Ret_Qty+PTRLength]), 64)
	tempItem.PurchaseReturn, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Purch_Ret_Qty+PTRLength]), 64)
	tempItem.Adjustments, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Adjustments_Qty+PTRLength]), 64)
	tempItem.ClosingStock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_ClosingQty+PTRLength]), 64)
	return tempItem
}
