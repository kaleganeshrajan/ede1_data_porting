package services

import (
	md "ede1_data_porting/models"
	ut "ede1_data_porting/utils"
)

//FileDetails insert file details 
func FileDetails(FilePath string, DistributorCode string, SS int, BT int, INV int, Processtime int64,TableName string) (err error) {
	var filedetails md.FileDetails
	filedetails.FilePath = FilePath
	filedetails.StockistCode = DistributorCode
	filedetails.RecordCount_SS = SS
	filedetails.RecordCount_BT = BT
	filedetails.RecordCount_INV = INV
	filedetails.FileProcessTime = Processtime
	testinter := filedetails
	err = ut.GenerateJsonFile(testinter, TableName)
	if err != nil {
		return err
	}
	return nil
}
