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
	startTime := time.Now()
	//log.Printf("Starting file parse: %v", g.FilePath)

	var records models.Record
	cMap := make(map[string]models.Company)
	var fd utils.FileDetail
	var cm models.Common

	records.FilePath = g.FilePath
	if strings.Contains(strings.ToUpper(g.FilePath), "CSV 1.0") {
		records.FileType = strconv.Itoa(headers.CSV_1_0)
	} else {
		records.FileType = strconv.Itoa(headers.CSV_1_1)
	}

	records.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	if strings.Contains(g.BucketName, "MTD") {
		records.Duration = headers.DurationMTD
	} else {
		records.Duration = headers.DurationMonthly
	}
	SS_count := 0

	for {
		line, err := reader.ReadString('\r')

		if err != nil && err == io.EOF {
			break
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
				log.Printf("stockandsales_csv From Date Error: %v : %v", err, lineSlice[headers.From_Date])
			} else {
				records.FromDate = cm.FromDate.Format("2006-01-02")
			}
			cm.ToDate, err = utils.ConvertDate(strings.TrimSpace(lineSlice[headers.To_Date]))
			if err != nil || cm.ToDate == nil {
				log.Printf("stockandsales_csv To Date Error: %v : %v", err, lineSlice[headers.To_Date])
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
	}

	var testinter interface{}
	if len(cMap) > 0 {
		for _, val := range cMap {
			records.Companies = append(records.Companies, val)
		}
		testinter = records
		err = utils.GenerateJsonFile(testinter, headers.Stock_and_Sales)
		if err != nil {
			return err
		}

		fd.FileDetails(g.FilePath, records.DistributorCode, SS_count, 0,
			0, int64(time.Since(startTime)/1000000), headers.File_details)
		if err != nil {
			return err
		}

		g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
		//log.Printf("File parsing done: %v", g.FilePath)

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
	tempItem.Item_code = strings.TrimSpace(lineSlice[headers.Csv_Stkt_Product_Code])
	SearchString, err := utils.ReplaceSpacialCharactor(strings.TrimSpace(lineSlice[headers.Csv_Stkt_Product_Code]))
	if err != nil {
		log.Printf("Error while replacing spacail charactor : %v\n", err)
	} else {
		tempItem.SearchString = SearchString
	}

	tempItem.Item_name = strings.TrimSpace(lineSlice[headers.Csv_Product_Name])
	tempItem.Pack = strings.TrimSpace(lineSlice[headers.Csv_Pack])
	if len(lineSlice) >= 16 {
		tempItem.PTR, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_PTR]), 64)
		PTRLength = 1
	}
	tempItem.Opening_stock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Opening_Qty+PTRLength]), 64)
	tempItem.Purchases_Reciepts, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Receipts_Qty+PTRLength]), 64)
	tempItem.Sales_qty, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Sales_Qty+PTRLength]), 64)
	tempItem.Sales_return, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Sales_Ret_Qty+PTRLength]), 64)
	tempItem.Purchase_return, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Purch_Ret_Qty+PTRLength]), 64)
	tempItem.Adjustments, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_Adjustments_Qty+PTRLength]), 64)
	tempItem.Closing_Stock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[headers.Csv_ClosingQty+PTRLength]), 64)
	return tempItem
}
