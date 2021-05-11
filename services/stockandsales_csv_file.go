package services

import (
	hd "ede1_data_porting/headers"
	md "ede1_data_porting/models"
	ut "ede1_data_porting/utils"
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"strings"

	"github.com/google/uuid"

	cr "github.com/brkelkar/common_utils/configreader"
)

func StockandSalesCSVParser(g ut.GcsFile, cfg cr.Config) (err error) {
	log.Printf("Starting file parse: %v", g.FilePath)
	Init()
	r := g.GcsClient.GetReader()
	reader := bufio.NewReader(r)
	if reader == nil {
		log.Println("error while getting reader")
		return
	}

	var records md.Record
	cMap := make(map[string]md.Company)
	PTRLength := 0

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

		switch lineSlice[0] {
		case "H":
			records.DistributorCode = strings.TrimSpace(lineSlice[hd.Stockist_Code])
			FromDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.From_Date]))
			records.FromDate = FromDate.Format("2006-01-02")
			ToDate, _ = ut.ConvertDate(strings.TrimSpace(lineSlice[hd.To_Date]))
			records.ToDate = ToDate.Format("2006-01-02")
		case "T":
			var tempItem md.Item
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

			if _, ok := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]; !ok {
				var tempCompany md.Company
				tempCompany.CompanyCode = strings.TrimSpace(lineSlice[hd.Company_code])
				tempCompany.CompanyName = strings.TrimSpace(lineSlice[hd.Company_name])
				cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = tempCompany
			}
			t := cMap[strings.TrimSpace(lineSlice[hd.Company_code])]
			t.Items = append(t.Items, tempItem)
			cMap[strings.TrimSpace(lineSlice[hd.Company_code])] = t
		}
	}

	for _, val := range cMap {
		records.Companies = append(records.Companies, val)
	}

	file, err := json.Marshal(records)
	if err != nil {
		panic(err)
	}

	Filename := hd.Filename + uuid.New().String() + ".json"

	err = ioutil.WriteFile(Filename, file, 0644)
	if err != nil {
		log.Printf("Error while creating Json file: %v", err)
	}

	err = ut.ImporttoBigquery(hd.ProjectID, hd.DatasetID, TableId[1], Filename)
	if err != nil {
		log.Printf("Error while importing to bigquery: %v", err)
	}

	log.Printf("File parse done: %v", g.FilePath)
	return err
}
