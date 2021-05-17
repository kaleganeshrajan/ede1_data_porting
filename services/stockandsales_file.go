package services

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

var (
	fd                   ut.FileDetail
	stockandsalesRecords md.Record
	batchRecords         md.RecordBatch
	invoicRrecords       md.RecordInvoice
)

//StockandSalesCSVParser stock and sales with PTS and without PTS, Batch and Invoice details data parse
func StockandSalesParser(g ut.GcsFile, cfg cr.Config) (err error) {
	startTime := time.Now()
	log.Printf("Starting file parse: %v", g.FilePath)
	Init()

	r := g.GcsClient.GetReader()
	reader := bufio.NewReader(r)
	if reader == nil {
		log.Println("error while getting reader")
		return
	}

	var stockandsalesRecords md.Record
	var batchRecords md.RecordBatch
	var invoicRrecords md.RecordInvoice

	cMap := make(map[string]md.Company)
	cMapInvoice := make(map[string]md.CompanyInvoice)

	AssignHeader(g)

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
			FromDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.From_Date]))
			stockandsalesRecords.FromDate = FromDate.Format("2006-01-02")
			batchRecords.FromDate = stockandsalesRecords.FromDate
			invoicRrecords.FromDate = stockandsalesRecords.FromDate
			ToDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.To_Date]))
			stockandsalesRecords.ToDate = ToDate.Format("2006-01-02")
			batchRecords.ToDate = stockandsalesRecords.ToDate
			invoicRrecords.ToDate = stockandsalesRecords.ToDate
			g.DistributorCode = stockandsalesRecords.DistributorCode
		case "T1":
			SS_count = SS_count + 1

			tempItem := AssignItemH1(lineSlice)

			if _, ok := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]; !ok {
				tempCompany := AssignCompanySS(lineSlice)
				cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = tempCompany
			}
			t := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]
			t.Items = append(t.Items, tempItem)
			cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = t

			if len(lineSlice) >= 24 {
				stockandsalesRecords.FileType = hd.FileTypePTS
				batchRecords.FileType = hd.FileTypePTS
				invoicRrecords.FileType = hd.FileTypePTS
			}
		case "T2":
			tempItem := AssignItemH2(lineSlice)
			batchRecords.Batches = append(batchRecords.Batches, tempItem)
		case "T3":
			INV_Count = INV_Count + 1
			tempItem := AssignItemH3(lineSlice)

			if _, ok := cMapInvoice[lineSlice[hd.Company_code]]; !ok {
				tempCompany := AssignCompanyinvocie(lineSlice)
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
		err = ut.GenerateJsonFile(testinter, TableId[1])
		if err != nil {
			return err
		}
	}

	if len(batchRecords.Batches) > 1 {
		testinter = batchRecords
		err = ut.GenerateJsonFile(testinter, TableId[2])
		if err != nil {
			return err
		}
	}

	if len(cMapInvoice) > 0 {
		for _, val := range cMapInvoice {
			invoicRrecords.Companies = append(invoicRrecords.Companies, val)
		}
		testinter = invoicRrecords
		err = ut.GenerateJsonFile(testinter, TableId[3])
		if err != nil {
			return err
		}
	}

	fd.FileDetails(g.FilePath, stockandsalesRecords.DistributorCode, SS_count, len(batchRecords.Batches),
		INV_Count, int64(time.Since(startTime)/1000000), TableId[4])

	g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-ported")
	log.Printf("File parsing done: %v", g.FilePath)

	g.TimeDiffrence = int64(time.Since(startTime) / 1000000)
	g.LogFileDetails(true)
	return err
}

func AssignHeader(g ut.GcsFile) {
	stockandsalesRecords.FilePath = g.FilePath
	batchRecords.FilePath = g.FilePath
	invoicRrecords.FilePath = g.FilePath

	stockandsalesRecords.FileType = hd.FileType
	batchRecords.FileType = hd.FileType
	invoicRrecords.FileType = hd.FileType

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

func AssignItemH1(lineSlice []string) (tempItem md.Item) {
	PTSLength := 0
	tempItem.Item_code = strings.TrimSpace(lineSlice[hd.Item_code])
	tempItem.Item_name = strings.TrimSpace(lineSlice[hd.Item_name])
	tempItem.Pack = strings.TrimSpace(lineSlice[hd.PACK])
	tempItem.UPC = strings.TrimSpace(lineSlice[hd.UPC])
	tempItem.PTR = strings.TrimSpace(lineSlice[hd.PTR])
	if len(lineSlice) >= 24 {
		tempItem.PTS = strings.TrimSpace(lineSlice[hd.PTS])
		PTSLength = 1
	}
	tempItem.MRP = strings.TrimSpace(lineSlice[hd.MRP+PTSLength])
	tempItem.Opening_stock = strings.TrimSpace(lineSlice[hd.Opening_stock+PTSLength])
	tempItem.Sales_qty = strings.TrimSpace(lineSlice[hd.Sales_Qty+PTSLength])
	tempItem.Bonus_qty = strings.TrimSpace(lineSlice[hd.Bonus_qty+PTSLength])
	tempItem.Sales_return = strings.TrimSpace(lineSlice[hd.Sales_Return+PTSLength])
	tempItem.Expiry_in = strings.TrimSpace(lineSlice[hd.Expiry_In+PTSLength])
	tempItem.Discount_percentage = strings.TrimSpace(lineSlice[hd.Discount_percentage+PTSLength])
	tempItem.Discount_amount = strings.TrimSpace(lineSlice[hd.Discount_amount+PTSLength])
	tempItem.Sale_tax = strings.TrimSpace(lineSlice[hd.Sale_tax+PTSLength])
	tempItem.Purchases_Reciepts = strings.TrimSpace(lineSlice[hd.Purchases_Reciepts+PTSLength])
	tempItem.Purchase_return = strings.TrimSpace(lineSlice[hd.Purchase_return+PTSLength])
	tempItem.Expiry_out = strings.TrimSpace(lineSlice[hd.Expiry_out+PTSLength])
	tempItem.Adjustments = strings.TrimSpace(lineSlice[hd.Adjustments+PTSLength])
	tempItem.Closing_Stock = strings.TrimSpace(lineSlice[hd.Closing_Stock+PTSLength])
	if len(lineSlice) >= 29 {
		PTSLength = 1
		tempItem.InstaSales = strings.TrimSpace(lineSlice[hd.InstaSales+PTSLength])
		tempItem.OpenVal = strings.TrimSpace(lineSlice[hd.OpenVal+PTSLength])
		tempItem.PurchaseVal = strings.TrimSpace(lineSlice[hd.PurchaseVal+PTSLength])
		tempItem.SalesVal = strings.TrimSpace(lineSlice[hd.SalesVal+PTSLength])
		tempItem.CloseVal = strings.TrimSpace(lineSlice[hd.CloseVal+PTSLength])
	}
	return tempItem
}

func AssignItemH2(lineSlice []string) (tempItem md.ItemBatch) {
	ExpiryDate, err := ut.ConvertDate(strings.TrimSpace(lineSlice[hd.H2_ExpiryDate]))
	if err != nil {
		ExpiryDate = &time.Time{}
		log.Printf("To expiry is not a correct format: %v", err)
	}

	tempItem.Item_name = strings.TrimSpace(lineSlice[hd.H2_Item_name])
	tempItem.Pack = strings.TrimSpace(lineSlice[hd.H2_PACK])
	tempItem.UPC = strings.TrimSpace(lineSlice[hd.H2_UPC])
	tempItem.Batch_number = strings.TrimSpace(lineSlice[hd.H2_BatchNumber])
	tempItem.Expiry_date = ExpiryDate.Format("2006-01-02")
	tempItem.Closing_Qty = strings.TrimSpace(lineSlice[hd.H2_Closing_Stock])
	return tempItem
}

func AssignItemH3(lineSlice []string) (tempItem md.Invoice) {
	InvoiceDate, err := ut.ConvertDate(strings.TrimSpace(lineSlice[hd.H3_Invoice_Date]))
	if err != nil {
		InvoiceDate = &time.Time{}
		log.Printf("To expiry is not a correct format: %v", err)
	}

	tempItem.Invoice_Number = lineSlice[hd.H3_Invoice_Number]
	tempItem.Invoice_Date = InvoiceDate.Format("2006-01-02")
	tempItem.Invoice_Amount = lineSlice[hd.H3_Invoice_amount]
	return tempItem
}

func AssignCompanySS(lineSlice []string) (tempCompany md.Company) {
	tempCompany.CompanyCode = strings.TrimSpace(lineSlice[hd.Company_code])
	tempCompany.CompanyName = strings.TrimSpace(lineSlice[hd.Company_name])
	return tempCompany
}

func AssignCompanyinvocie(lineSlice []string) (tempCompany md.CompanyInvoice) {
	tempCompany.CompanyCode = lineSlice[hd.Company_code]
	tempCompany.CompanyName = lineSlice[hd.Company_name]
	return tempCompany
}
