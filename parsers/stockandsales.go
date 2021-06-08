package parsers

import (
	"bufio"
	hd "ede_porting/headers"
	md "ede_porting/models"
	ut "ede_porting/utils"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

//StockandSalesCSVParser stock and sales with PTS and without PTS, Batch and Invoice details data parse
func StockandSalesParser(g ut.GcsFile, reader *bufio.Reader) (err error) {
	startTime := time.Now()
	//log.Printf("Starting file parse: %v", g.FilePath)

	var fd ut.FileDetail
	var stockandsalesRecords md.Record
	var batchRecords md.RecordBatch
	var invoicRrecords md.RecordInvoice
	var cm md.Common

	cMap := make(map[string]md.Company)
	cMapInvoice := make(map[string]md.CompanyInvoice)

	assignHeader(g, &stockandsalesRecords, &batchRecords, &invoicRrecords)

	SS_count := 0
	INV_Count := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		if len(line) <= 2 {
			break
		}

		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, "|")

		switch lineSlice[0] {
		case "H1", "H2", "H3":
			stockandsalesRecords.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockist_Code])
			batchRecords.DistributorCode = stockandsalesRecords.DistributorCode
			invoicRrecords.DistributorCode = stockandsalesRecords.DistributorCode

			cm.FromDate, err = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.From_Date]))
			if err != nil {
				log.Printf("stockandsales From Date Error: %v : %v", err, lineSlice[hd.From_Date])
			} else {
				stockandsalesRecords.FromDate = cm.FromDate.Format("2006-01-02")
				batchRecords.FromDate = stockandsalesRecords.FromDate
				invoicRrecords.FromDate = stockandsalesRecords.FromDate
			}
			cm.ToDate, err = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.To_Date]))
			if err != nil {
				log.Printf("stockandsales To Date Error: %v : %v", err, lineSlice[hd.To_Date])
			} else {
				stockandsalesRecords.ToDate = cm.ToDate.Format("2006-01-02")
				batchRecords.ToDate = stockandsalesRecords.ToDate
				invoicRrecords.ToDate = stockandsalesRecords.ToDate
			}
			g.DistributorCode = stockandsalesRecords.DistributorCode
		case "T1":
			SS_count = SS_count + 1

			tempItem := assignItemH1(lineSlice)

			if _, ok := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]; !ok {
				tempCompany := assignCompanySS(lineSlice)
				cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = tempCompany
			}
			t := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]
			t.Items = append(t.Items, tempItem)
			cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = t

			if len(lineSlice) >= 24 {
				stockandsalesRecords.FileType = g.FileType
				batchRecords.FileType = g.FileType
				invoicRrecords.FileType = g.FileType
			}
		case "T2":
			tempItem := assignItemH2(lineSlice)
			batchRecords.Batches = append(batchRecords.Batches, tempItem)
		case "T3":
			INV_Count = INV_Count + 1
			tempItem := assignItemH3(lineSlice)

			if _, ok := cMapInvoice[lineSlice[hd.Company_code]]; !ok {
				tempCompany := assignCompanyinvocie(lineSlice)
				cMapInvoice[lineSlice[hd.Company_code]] = tempCompany
			}
			t := cMapInvoice[lineSlice[hd.Company_code]]
			t.Invoices = append(t.Invoices, tempItem)
			cMapInvoice[lineSlice[hd.Company_code]] = t
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

	if len(batchRecords.Batches) > 1 {
		testinter = batchRecords
		err = ut.GenerateJsonFile(testinter, hd.Batch_details)
		if err != nil {
			return err
		}
	}

	if len(cMapInvoice) > 0 {
		for _, val := range cMapInvoice {
			invoicRrecords.Companies = append(invoicRrecords.Companies, val)
		}
		testinter = invoicRrecords
		err = ut.GenerateJsonFile(testinter, hd.Invoice_details)
		if err != nil {
			return err
		}
	}

	if len(cMap) > 0 {
		fd.FileDetails(g.FilePath, stockandsalesRecords.DistributorCode, SS_count, len(batchRecords.Batches),
			INV_Count, int64(time.Since(startTime)/1000000), hd.File_details)

		g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
		//log.Printf("File parsing done: %v", g.FilePath)

		g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
		//g.LogFileDetails(true)
	} else {
		return errors.New("file is empty")
	}
	return err
}

func assignHeader(g ut.GcsFile, stockandsalesRecords *md.Record, batchRecords *md.RecordBatch, invoicRrecords *md.RecordInvoice) {
	stockandsalesRecords.FilePath = g.FilePath
	batchRecords.FilePath = g.FilePath
	invoicRrecords.FilePath = g.FilePath

	stockandsalesRecords.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	batchRecords.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	invoicRrecords.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")

	stockandsalesRecords.FileType = g.FileType
	batchRecords.FileType = g.FileType
	invoicRrecords.FileType = g.FileType

	if strings.Contains(g.BucketName, "MTD") {
		invoicRrecords.Duration = hd.DurationMTD
		batchRecords.Duration = hd.DurationMTD
		stockandsalesRecords.Duration = hd.DurationMTD
	} else {
		invoicRrecords.Duration = hd.DurationMonthly
		batchRecords.Duration = hd.DurationMonthly
		stockandsalesRecords.Duration = hd.DurationMonthly
	}
}

func assignItemH1(lineSlice []string) (tempItem md.Item) {
	PTSLength := 0
	tempItem.Item_code = strings.TrimSpace(lineSlice[hd.Item_code])
	tempItem.Item_name = strings.TrimSpace(lineSlice[hd.Item_name])
	tempItem.Pack = strings.TrimSpace(lineSlice[hd.PACK])
	tempItem.UPC = strings.TrimSpace(lineSlice[hd.UPC])
	tempItem.PTR, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.PTR]), 64)
	if len(lineSlice) >= 24 {
		tempItem.PTS, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.PTS]), 64)
		PTSLength = 1
	}
	tempItem.MRP, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.MRP+PTSLength]), 64)
	tempItem.Opening_stock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Opening_stock+PTSLength]), 64)
	tempItem.Sales_qty, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Sales_Qty+PTSLength]), 64)
	tempItem.Bonus_qty, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Bonus_qty+PTSLength]), 64)
	tempItem.Sales_return, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Sales_Return+PTSLength]), 64)
	tempItem.Expiry_in, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Expiry_In+PTSLength]), 64)
	tempItem.Discount_percentage, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Discount_percentage+PTSLength]), 64)
	tempItem.Discount_amount, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Discount_amount+PTSLength]), 64)
	tempItem.Sale_tax, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Sale_tax+PTSLength]), 64)
	tempItem.Purchases_Reciepts, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Purchases_Reciepts+PTSLength]), 64)
	tempItem.Purchase_return, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Purchase_return+PTSLength]), 64)
	tempItem.Expiry_out, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Expiry_out+PTSLength]), 64)
	tempItem.Adjustments, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Adjustments+PTSLength]), 64)
	tempItem.Closing_Stock, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.Closing_Stock+PTSLength]), 64)
	if len(lineSlice) >= 29 {
		PTSLength = 1
		tempItem.InstaSales, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.InstaSales+PTSLength]), 64)
		tempItem.OpenVal, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.OpenVal+PTSLength]), 64)
		tempItem.PurchaseVal, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.PurchaseVal+PTSLength]), 64)
		tempItem.SalesVal, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.SalesVal+PTSLength]), 64)
		tempItem.CloseVal, _ = strconv.ParseFloat(strings.TrimSpace(lineSlice[hd.CloseVal+PTSLength]), 64)
	}
	return tempItem
}

func assignItemH2(lineSlice []string) (tempItem md.ItemBatch) {
	tempItem.Item_name = strings.TrimSpace(lineSlice[hd.H2_Item_name])
	tempItem.Pack = strings.TrimSpace(lineSlice[hd.H2_PACK])
	tempItem.UPC = strings.TrimSpace(lineSlice[hd.H2_UPC])
	tempItem.Batch_number = strings.TrimSpace(lineSlice[hd.H2_BatchNumber])

	ExpiryDate, err := ut.ConvertDate(strings.TrimSpace(lineSlice[hd.H2_ExpiryDate]))
	if err != nil {
		log.Printf("stockandsales Expiry Date Error: %v : %v", err, lineSlice[hd.H2_ExpiryDate])
	} else {
		tempItem.Expiry_date = ExpiryDate.Format("2006-01-02")
	}

	tempItem.Closing_Qty = strings.TrimSpace(lineSlice[hd.H2_Closing_Stock])
	return tempItem
}

func assignItemH3(lineSlice []string) (tempItem md.Invoice) {
	tempItem.Invoice_Number = lineSlice[hd.H3_Invoice_Number]
	InvoiceDate, err := ut.ConvertDate(strings.TrimSpace(lineSlice[hd.H3_Invoice_Date]))
	if err != nil {
		log.Printf("stockandsales Invoice Date Error: %v : %v", err, lineSlice[hd.H3_Invoice_Date])
	} else {
		tempItem.Invoice_Date = InvoiceDate.Format("2006-01-02")
	}
	tempItem.Invoice_Amount = lineSlice[hd.H3_Invoice_amount]
	return tempItem
}

func assignCompanySS(lineSlice []string) (tempCompany md.Company) {
	tempCompany.CompanyCode = strings.TrimSpace(lineSlice[hd.Company_code])
	tempCompany.CompanyName = strings.TrimSpace(lineSlice[hd.Company_name])
	return tempCompany
}

func assignCompanyinvocie(lineSlice []string) (tempCompany md.CompanyInvoice) {
	tempCompany.CompanyCode = lineSlice[hd.Company_code]
	tempCompany.CompanyName = lineSlice[hd.Company_name]
	return tempCompany
}
