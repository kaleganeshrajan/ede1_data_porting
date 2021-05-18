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

//StockandSalesCSVParser stock and sales with PTS and without PTS, Batch and Invoice details data parse
func StockandSalesSale(g ut.GcsFile, cfg cr.Config, reader *bufio.Reader) (err error) {
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
			SS_count = SS_count + 1

			tempItem := assignItem(lineSlice)
			g.DistributorCode = stockandsalesRecords.DistributorCode

			if _, ok := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]; !ok {
				var tempCompany md.Company
				tempCompany.CompanyCode = strings.TrimSpace(lineSlice[hd.CompanyCode])
				tempCompany.CompanyName = strings.TrimSpace(lineSlice[hd.CompanyName])
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

	fd.FileDetails(g.FilePath, stockandsalesRecords.DistributorCode, SS_count, 0,
		0, int64(time.Since(startTime)/1000000), hd.File_details)

	g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
	log.Printf("File parsing done: %v", g.FilePath)

	g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
	g.LogFileDetails(true)

	return err
}

func assignHeaders(g ut.GcsFile) {
	stockandsalesRecords.Key = g.FileKey
	stockandsalesRecords.FilePath = g.FilePath
	stockandsalesRecords.FileType = hd.FileTypewithPTR
	if strings.Contains(g.BucketName, "MTD") {
		stockandsalesRecords.Duration = hd.DurationMTD
	} else {
		stockandsalesRecords.Duration = hd.DurationMonthly
	}
}

func assignItem(lineSlice []string) (tempItem md.Item) {
	stockandsalesRecords.DistributorCode = strings.TrimSpace(lineSlice[hd.StockistCode])
	cm.FromDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.FromDate]))
	stockandsalesRecords.FromDate = cm.FromDate.Format("2006-01-02")
	cm.ToDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.ToDate]))
	stockandsalesRecords.ToDate = cm.ToDate.Format("2006-01-02")

	tempItem.Item_code = strings.TrimSpace(lineSlice[hd.ItemCode])
	tempItem.Item_name = strings.TrimSpace(lineSlice[hd.ItemName])
	tempItem.Pack = strings.TrimSpace(lineSlice[hd.PackSize])
	tempItem.PTR = strings.TrimSpace(lineSlice[hd.SprPTR])
	tempItem.Opening_stock = strings.TrimSpace(lineSlice[hd.OpeningStock])
	tempItem.Sales_qty = strings.TrimSpace(lineSlice[hd.SalesQty])
	tempItem.Bonus_qty = strings.TrimSpace(lineSlice[hd.BonusQty])
	tempItem.Discount_percentage = strings.TrimSpace(lineSlice[hd.DiscountPer])
	tempItem.Discount_amount = strings.TrimSpace(lineSlice[hd.DiscountAmount])
	tempItem.Closing_Stock = strings.TrimSpace(lineSlice[hd.ClosingStock])
	tempItem.Sales_return = strings.TrimSpace(lineSlice[hd.SretQty])
	tempItem.Sale_tax = strings.TrimSpace(lineSlice[hd.StaxPerc])

	return tempItem
}
