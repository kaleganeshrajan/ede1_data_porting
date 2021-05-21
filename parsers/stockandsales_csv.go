package parsers

import (
	"bufio"
	hd "ede_porting/headers"
	md "ede_porting/models"
	ut "ede_porting/utils"
	"io"
	"log"
	"strings"
	"time"

	cr "github.com/brkelkar/common_utils/configreader"
)

//StockandSalesParser parse stock and sales with PTR and without PTR
func StockandSalesCSVParser(g ut.GcsFile, cfg cr.Config) (err error) {
	startTime := time.Now()
	log.Printf("Starting file parse: %v", g.FilePath)

	r := g.GcsClient.GetReader()
	reader := bufio.NewReader(r)

	if reader == nil {
		log.Println("error while getting reader")
		return
	}

	var records md.Record
	cMap := make(map[string]md.Company)
	var fd ut.FileDetail
	var cm md.Common

	records.FilePath = g.FilePath
	records.FileType = hd.FileType
	records.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	if strings.Contains(g.BucketName, "MTD") {
		records.Duration = hd.DurationMTD
	} else {
		records.Duration = hd.DurationMonthly
	}
	SS_count := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}

		if len(line) <= 2 {
			break
		}

		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, ",")

		lineSlice[0] = ut.Removespacialcharactor(lineSlice[0])

		switch lineSlice[0] {
		case "H":
			records.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockist_Code])
			cm.FromDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.From_Date]))
			records.FromDate = cm.FromDate.Format("2006-01-02")
			cm.ToDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.To_Date]))
			records.ToDate = cm.ToDate.Format("2006-01-02")
		case "T":
			SS_count = SS_count + 1
			tempItem := AssignItem(lineSlice)

			if _, ok := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]; !ok {
				tempCompany := assignCompanySS(lineSlice)
				cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = tempCompany
			}
			t := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]
			t.Items = append(t.Items, tempItem)
			cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = t

			if len(lineSlice) >= 16 {
				records.FileType = hd.FileTypePTR
			}
		}
	}

	var testinter interface{}
	if len(cMap) > 0 {
		for _, val := range cMap {
			records.Companies = append(records.Companies, val)
		}
		testinter = records
		err = ut.GenerateJsonFile(testinter, hd.Stock_and_Sales)
		if err != nil {
			return err
		}
	}

	fd.FileDetails(g.FilePath, records.DistributorCode, SS_count, 0,
		0, int64(time.Since(startTime)/1000000), hd.File_details)
	if err != nil {
		return err
	}

	g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
	log.Printf("File parsing done: %v", g.FilePath)

	g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
	g.LogFileDetails(true)
	return err
}

func AssignItem(lineSlice []string) (tempItem md.Item) {
	PTRLength := 0
	tempItem.UniformPdtCode = strings.TrimSpace(lineSlice[hd.Csv_Uniform_Pdt_Code])
	tempItem.Item_code = strings.TrimSpace(lineSlice[hd.Csv_Stkt_Product_Code])
	tempItem.Item_name = strings.TrimSpace(lineSlice[hd.Csv_Product_Name])
	tempItem.Pack = strings.TrimSpace(lineSlice[hd.Csv_Pack])
	if len(lineSlice) >= 16 {
		tempItem.PTR = strings.TrimSpace(lineSlice[hd.Csv_PTR])
		PTRLength = 1
	}
	tempItem.Opening_stock = strings.TrimSpace(lineSlice[hd.Csv_Opening_Qty+PTRLength])
	tempItem.Purchases_Reciepts = strings.TrimSpace(lineSlice[hd.Csv_Receipts_Qty+PTRLength])
	tempItem.Sales_qty = strings.TrimSpace(lineSlice[hd.Csv_Sales_Qty+PTRLength])
	tempItem.Sales_return = strings.TrimSpace(lineSlice[hd.Csv_Sales_Ret_Qty+PTRLength])
	tempItem.Purchase_return = strings.TrimSpace(lineSlice[hd.Csv_Purch_Ret_Qty+PTRLength])
	tempItem.Adjustments = strings.TrimSpace(lineSlice[hd.Csv_Adjustments_Qty+PTRLength])
	tempItem.Closing_Stock = strings.TrimSpace(lineSlice[hd.Csv_ClosingQty+PTRLength])
	return tempItem
}
