package parsers

import (
	"bufio"
	hd "ede1_data_porting/headers"
	md "ede1_data_porting/models"
	ut "ede1_data_porting/utils"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	cr "github.com/brkelkar/common_utils/configreader"
)

func StockandSalesDetails(g ut.GcsFile, cfg cr.Config, reader *bufio.Reader) (err error) {
	startTime := time.Now()
	log.Printf("Starting file parse: %v", g.FilePath)

	// r := g.GcsClient.GetReader()
	// reader := bufio.NewReader(r)
	if reader == nil {
		log.Println("error while getting reader")
		return
	}

	cMap := make(map[string]md.Company)

	assignHeaders(g)

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
		}

		if flag == 1 {
			flag = 0
		} else {
			SS_count = SS_count + 1

			tempItem := assignStandardItem(lineSlice)
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
	g.LogFileDetails(true)

	return err
}

func assignStandardItem(lineSlice []string) (tempItem md.Item) {
	if len(lineSlice) < 10 {
		fmt.Println(lineSlice)
	}
	stockandsalesRecords.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockistcode])
	cm.FromDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.Fromdate]))
	stockandsalesRecords.FromDate = cm.FromDate.Format("2006-01-02")
	cm.ToDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.Todate]))
	stockandsalesRecords.ToDate = cm.ToDate.Format("2006-01-02")

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
