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

var (
	colName []string
	colMap  map[string]int
)

func initParser() {
	colName = []string{"ITEM_CODE", "ITEM_NAME", "MANU_CODE", "MANU_NAME", "SALE_QTY", "BON_QTY", "DIS_PERC", "DIS_AMT", "SPR_PTR", "STAX_PERC", "SRET_QTY", "PACK_SIZE", "ACODE", "OP_BAL", "CL_BAL", "FROM_DATE", "TO_DATE"}
	colMap = make(map[string]int)
	for _, val := range colName {
		colMap[val] = -1
	}
}

//StockandSalesCSVParser stock and sales with PTS and without PTS, Batch and Invoice details data parse
func StockandSalesSale(g utils.GcsFile, reader *bufio.Reader) (err error) {
	startTime := time.Now()
	//log.Printf("Starting file parse: %v", g.FilePath)
	initParser()
	var fd utils.FileDetail
	var stockandsalesRecords md.Record

	cMap := make(map[string]md.Company)
	itemMap := make(map[string]md.SaleDist)

	SS_count := 0
	flag := 1
	seperator := "\x10"
	newLine := byte('\n')
	for {
		line, err := reader.ReadString(newLine)

		if err != nil && len(line) > 100 {
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
				seperator = ";"
				lineSlice = strings.Split(line, seperator)
			}
		}

		if len(lineSlice) == 17 {
			SS_count = SS_count + 1
			var tempSales md.SaleDist
			for index, val := range lineSlice {
				if flag == 1 {
					colMap[strings.ToUpper(strings.TrimSpace(val))] = index
				} else {
					switch index {
					case -1:
						break
					case colMap["ITEM_CODE"]:
						tempSales.ITEM_CODE = strings.TrimSpace(val)
					case colMap["ITEM_NAME"]:
						tempSales.ITEM_NAME = strings.TrimSpace(val)
					case colMap["MANU_CODE"]:
						tempSales.MANU_CODE = strings.TrimSpace(val)
					case colMap["MANU_NAME"]:
						tempSales.MANU_NAME = strings.TrimSpace(val)
					case colMap["SALE_QTY"]:
						tempSales.SALE_QTY = strings.TrimSpace(val)
					case colMap["BON_QTY"]:
						tempSales.BON_QTY = strings.TrimSpace(val)
					case colMap["DIS_PERC"]:
						tempSales.DIS_PERC = strings.TrimSpace(val)
					case colMap["DIS_AMT"]:
						tempSales.DIS_AMT = strings.TrimSpace(val)
					case colMap["SPR_PTR"]:
						tempSales.SPR_PTR = strings.TrimSpace(val)
					case colMap["STAX_PERC"]:
						tempSales.STAX_PERC = strings.TrimSpace(val)
					case colMap["SRET_QTY"]:
						tempSales.SRET_QTY = strings.TrimSpace(val)
					case colMap["PACK_SIZE"]:
						tempSales.PACK_SIZE = strings.TrimSpace(val)
					case colMap["ACODE"]:
						tempSales.ACODE = strings.TrimSpace(val)
					case colMap["OP_BAL"]:
						tempSales.OP_BAL = strings.TrimSpace(val)
					case colMap["CL_BAL"]:
						tempSales.CL_BAL = strings.TrimSpace(val)
					case colMap["FROM_DATE"]:
						tempSales.FROM_DATE = strings.TrimSpace(val)
					case colMap["TO_DATE"]:
						tempSales.TO_DATE = strings.TrimSpace(val)
					}
				}
			}
			if flag == 0 {
				itemMap[tempSales.ACODE+tempSales.ITEM_CODE] = tempSales
			}

			flag = 0

		} else {
			return errors.New("file is not correct format")
		}

		if err != nil && err == io.EOF {
			break
		}

	}

	assignHeaders(g, &stockandsalesRecords)

	for _, lineSlice := range itemMap {
		SS_count = SS_count + 1

		tempItem := assignItem(lineSlice, &stockandsalesRecords)
		g.DistributorCode = stockandsalesRecords.DistributorCode

		if _, ok := cMap[strings.TrimSpace(lineSlice.MANU_CODE)]; !ok {
			var tempCompany md.Company
			tempCompany.CompanyCode = strings.TrimSpace(lineSlice.MANU_CODE)
			tempCompany.CompanyName = strings.TrimSpace(lineSlice.MANU_NAME)
			cMap[strings.TrimSpace(lineSlice.MANU_CODE)] = tempCompany
		}
		t := cMap[strings.TrimSpace(lineSlice.MANU_CODE)]
		t.Items = append(t.Items, tempItem)
		cMap[strings.TrimSpace(lineSlice.MANU_CODE)] = t
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

		fd.FileDetails(g.FilePath, stockandsalesRecords.DistributorCode, SS_count, 0,
			0, int64(time.Since(startTime)/1000000), hd.File_details)

		g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
		//log.Printf("File parsing done: %v", g.FilePath)

		g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
		//g.LogFileDetails(true)
	} else {
		return errors.New("file is empty")
	}

	return nil
}

func assignHeaders(g utils.GcsFile, stockandsalesRecords *md.Record) {
	stockandsalesRecords.FilePath = g.FilePath
	if strings.Contains(strings.ToUpper(g.FilePath), "STANDARD V4 PATCH") {
		stockandsalesRecords.FileType = strconv.Itoa(hd.Standard_V4_Patch)
	} else if strings.Contains(strings.ToUpper(g.FilePath), "STANDARD V5 PATCH") {
		stockandsalesRecords.FileType = strconv.Itoa(hd.Standard_V5_Patch)
	} else {
		stockandsalesRecords.FileType = strconv.Itoa(hd.Standard_Excel_Format)
	}

	stockandsalesRecords.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	if strings.Contains(g.BucketName, "MTD") {
		stockandsalesRecords.Duration = hd.DurationMTD
	} else {
		stockandsalesRecords.Duration = hd.DurationMonthly
	}
}

func assignItem(lineSlice md.SaleDist, stockandsalesRecords *md.Record) (tempItem md.Item) {
	var cm md.Common
	var err error
	stockandsalesRecords.DistributorCode = strings.TrimSpace(lineSlice.ACODE)

	cm.FromDate, err = utils.ConvertDate(strings.TrimSpace(lineSlice.FROM_DATE))
	if err != nil || cm.FromDate == nil {
		log.Printf("stockandsales_sale From Date Error: %v : %v", err, lineSlice.FROM_DATE)
	} else {
		stockandsalesRecords.FromDate = cm.FromDate.Format("2006-01-02")
	}
	cm.ToDate, _ = utils.ConvertDate(strings.TrimSpace(lineSlice.TO_DATE))
	if err != nil || cm.ToDate == nil {
		log.Printf("stockandsales_sale To Date Error: %v : %v", err, lineSlice.TO_DATE)
	} else {
		stockandsalesRecords.ToDate = cm.ToDate.Format("2006-01-02")
	}

	tempItem.ItemCode = strings.TrimSpace(lineSlice.ITEM_CODE)
	tempItem.ItemName = strings.TrimSpace(lineSlice.ITEM_NAME)
	SearchString, err := utils.ReplaceSpacialCharactor(strings.TrimSpace(lineSlice.ITEM_NAME))
	if err != nil {
		log.Printf("Error while replacing spacail charactor : %v\n", err)
	} else {
		tempItem.SearchString = SearchString
	}
	tempItem.Pack = strings.TrimSpace(lineSlice.PACK_SIZE)
	tempItem.PTR, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.SPR_PTR), 64)
	tempItem.OpeningStock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.OP_BAL), 64)
	tempItem.SalesQty, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.SALE_QTY), 64)
	tempItem.BonusQty, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.BON_QTY), 64)
	tempItem.DiscountPercentage, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.DIS_PERC), 64)
	tempItem.DiscountAmount, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.DIS_AMT), 64)
	tempItem.ClosingStock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.CL_BAL), 64)
	tempItem.SalesReturn, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.SALE_QTY), 64)
	tempItem.SaleTax, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice.STAX_PERC), 64)

	return tempItem
}
