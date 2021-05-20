package parsers

import (
	"bufio"
	hd "ede1_data_porting/headers"
	md "ede1_data_porting/models"
	ut "ede1_data_porting/utils"
	"errors"
	"io"
	"log"
	"strings"
	"time"

	cr "github.com/brkelkar/common_utils/configreader"
)

func StockandSalesDetails(g ut.GcsFile, cfg cr.Config, reader *bufio.Reader) (err error) {
	startTime := time.Now()
	log.Printf("Starting file parse: %v", g.FilePath)

	if reader == nil {
		log.Println("error while getting reader")
		return errors.New("error while getting reader")
	}

	cMap := make(map[string]md.Company)

	var stockandsalesRecords md.Record
	var fd ut.FileDetail

	assignHeaders(g, &stockandsalesRecords)

	SS_count := 0
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
			if len(lineSlice) <= 3 {
				return errors.New("FIle format is wrong " + g.FileName)
			}
		}

		if flag == 1 {
			flag = 0
		} else {
			if len(lineSlice) < 14 {
				log.Println("File is not correct format")
				return nil
			}
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
		}
	}

	var testinter interface{}
	if len(cMap) > 0 {
		for _, val := range cMap {
			stockandsalesRecords.Companies = append(stockandsalesRecords.Companies, val)
		}
		testinter = stockandsalesRecords
		err = ut.GenerateJsonFile(testinter, hd.Stock_and_Sales)
		if err != nil {
			return err
		}
	}

	fd.FileDetails(g.FilePath, stockandsalesRecords.DistributorCode, SS_count, 0, 0, int64(time.Since(startTime)/1000000), hd.File_details)

	g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
	log.Printf("File parsing done: %v", g.FilePath)

	g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
	//g.LogFileDetails(true)

	return nil
}

func assignStandardItem(lineSlice []string, stockandsalesRecords *md.Record) (tempItem md.Item) {
	var cm md.Common
	var err error
	stockandsalesRecords.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockistcode])
	cm.FromDate, err = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.Fromdate]))
	log.Println("0 : " + strings.TrimSpace(lineSlice[0]))
	log.Println("1 : " + strings.TrimSpace(lineSlice[1]))
	log.Println("2 : " + strings.TrimSpace(lineSlice[2]))
	log.Println("3 : " + strings.TrimSpace(lineSlice[3]))
	log.Println("4 : " + strings.TrimSpace(lineSlice[4]))
	log.Println("5 : " + strings.TrimSpace(lineSlice[5]))
	log.Println("6 : " + strings.TrimSpace(lineSlice[6]))
	log.Println("7 : " + strings.TrimSpace(lineSlice[7]))
	log.Println("8 : " + strings.TrimSpace(lineSlice[8]))
	log.Println("9 : " + strings.TrimSpace(lineSlice[9]))
	log.Println("10 : " + strings.TrimSpace(lineSlice[10]))
	log.Println("11 : " + strings.TrimSpace(lineSlice[11]))
	log.Println("12 : " + strings.TrimSpace(lineSlice[12]))
	if err != nil {
		log.Printf("CM From Date Error: %v : %v", err, lineSlice[hd.Fromdate])
	} else {
		stockandsalesRecords.FromDate = cm.FromDate.Format("2006-01-02")
	}
	cm.ToDate, err = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.Todate]))
	if err != nil {
		log.Printf("CM To Date Error: %v : %v", err, lineSlice[hd.Todate])
	} else {
		stockandsalesRecords.ToDate = cm.ToDate.Format("2006-01-02")
	}
	tempItem.Item_name = strings.TrimSpace(lineSlice[hd.ProductName])
	tempItem.PTR = strings.TrimSpace(lineSlice[hd.StandardPTR])
	tempItem.Opening_stock = strings.TrimSpace(lineSlice[hd.OpeingUnits])
	tempItem.Sales_qty = strings.TrimSpace(lineSlice[hd.SalesUnits])
	tempItem.Closing_Stock = strings.TrimSpace(lineSlice[hd.ClosingUnits])
	tempItem.PurchaseVal = strings.TrimSpace(lineSlice[hd.PurchaseUnits])
	tempItem.Purchase_return = strings.TrimSpace(lineSlice[hd.PurchaseReturn])
	tempItem.Sales_return = strings.TrimSpace(lineSlice[hd.SalesReturn])
	tempItem.PurchaseFree = strings.TrimSpace(lineSlice[hd.PurchaseFree])
	tempItem.SalesFree = strings.TrimSpace(lineSlice[hd.SalesFree])

	return tempItem
}
