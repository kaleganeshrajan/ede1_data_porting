package services

import (
	md "ede1_data_porting/models"
	ut "ede1_data_porting/utils"
	"time"
)

//FileDetails insert file details
func FileDetails(FilePath string, DistributorCode string, SS int, BT int, INV int, Processtime int64, TableName string) (err error) {
	var filedetails md.FileDetails
	filedetails.FilePath = FilePath
	filedetails.StockistCode = DistributorCode
	filedetails.RecordCount_SS = SS
	filedetails.RecordCount_BT = BT
	filedetails.RecordCount_INV = INV
	filedetails.FileProcessTime = Processtime
	filedetails.CreationDatetime = time.Now().Format("2006-01-02 15:04:05")
	testinter := filedetails
	err = ut.GenerateJsonFile(testinter, TableName)
	if err != nil {
		return err
	}
	return nil
}
