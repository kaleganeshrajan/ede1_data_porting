package parsers

import (
	"bufio"
	hd "ede_porting/headers"
	md "ede_porting/models"
	"ede_porting/utils"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

func StockandSalesDetails(g utils.GcsFile, reader *bufio.Reader) (err error) {
	startTime := time.Now()
	//log.Printf("Starting file parse: %v", g.FilePath)

	cMap := make(map[string]md.Company)

	var stockandsalesRecords md.Record
	var fd utils.FileDetail

	assignHeaders(g, &stockandsalesRecords)

	SS_count := 0
	flag := 1
	seperator := "\x10"
	newLine := byte('\n')
	for {
		line, err := reader.ReadString(newLine)

		if err != nil && len(line) > 1000 {
			reader = bufio.NewReader(strings.NewReader(line))
			newLine = '\r'
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
				return errors.New("File format is wrong :- " + line)
			}
		}

		if flag == 1 {
			flag = 0
		} else {
			if len(lineSlice) == 14 {
				if len(strings.TrimSpace(lineSlice[hd.Stockistcode])) > 1 {
					SS_count = SS_count + 1

					tempItem := assignStandardItem(lineSlice, &stockandsalesRecords)
					g.DistributorCode = stockandsalesRecords.DistributorCode

					if _, ok := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]; !ok {
						var tempCompany md.Company
						tempCompany.CompanyName = strings.TrimSpace(lineSlice[hd.Companyname])
						cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = tempCompany
					}
					t := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]
					t.Items = append(t.Items, tempItem)
					cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = t
				}
			} else {
				return errors.New("file is not correct format")
			}
		}

		
		if err != nil && err == io.EOF {
			break
		}


	}

	var testinter interface{}
	if len(cMap) > 0 {
		for _, val := range cMap {
			stockandsalesRecords.Companies = append(stockandsalesRecords.Companies, val)
		}
		testinter = stockandsalesRecords
		err = utils.GenerateJsonFile(testinter, hd.Stock_and_Sales)
		if err != nil {
			return err
		}

		fd.FileDetails(g.FilePath, stockandsalesRecords.DistributorCode, SS_count, 0, 0, int64(time.Since(startTime)/1000000), hd.File_details)

		g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
		//log.Printf("File parsing done: %v", g.FilePath)

		g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
		//g.LogFileDetails(true)
	} else {
		return errors.New("file is empty")
	}

	return nil
}

func assignStandardItem(lineSlice []string, stockandsalesRecords *md.Record) (tempItem md.Item) {
	var cm md.Common
	var err error
	stockandsalesRecords.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockistcode])
	stockandsalesRecords.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	cm.FromDate, err = utils.ConvertDate(strings.TrimSpace(lineSlice[hd.Fromdate]))
	if err != nil || cm.FromDate == nil {
		log.Printf("stockandsales_details From Date Error: %v : %v", err, lineSlice[hd.Fromdate])
	} else {
		stockandsalesRecords.FromDate = cm.FromDate.Format("2006-01-02")
	}
	cm.ToDate, err = utils.ConvertDate(strings.TrimSpace(lineSlice[hd.Todate]))
	if err != nil || cm.ToDate == nil {
		log.Printf("stockandsales_details To Date Error: %v : %v", err, lineSlice[hd.Todate])
	} else {
		stockandsalesRecords.ToDate = cm.ToDate.Format("2006-01-02")
	}
	tempItem.ItemName = strings.TrimSpace(lineSlice[hd.ProductName])
	SearchString, err := utils.ReplaceSpacialCharactor(strings.TrimSpace(lineSlice[hd.ProductName]))
	if err != nil {
		log.Printf("Error while replacing spacail charactor : %v\n", err)
	} else {
		tempItem.SearchString = SearchString
	}

	tempItem.PTR, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.StandardPTR]), 64)
	tempItem.OpeningStock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.OpeingUnits]), 64)
	tempItem.SalesQty, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.SalesUnits]), 64)
	tempItem.ClosingStock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.ClosingUnits]), 64)
	tempItem.PurchaseVal, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.PurchaseUnits]), 64)
	tempItem.PurchaseReturn, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.PurchaseReturn]), 64)
	tempItem.SalesReturn, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.SalesReturn]), 64)
	tempItem.PurchaseFree, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.PurchaseFree]), 64)
	tempItem.SalesFree, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.SalesFree]), 64)

	return tempItem
}
