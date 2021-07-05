package utils

import (
	"log"
	"time"
)

type FileDetail struct {
	FilePath         string `json:"FilePath"`
	StockistCode     string `json:"StockistCode"`
	FileProcessTime  int64  `json:"FileProcessTime"`
	RecordCount_SS   int    `json:"RecordCount_SS"`
	RecordCount_BT   int    `json:"RecordCount_BT"`
	RecordCount_INV  int    `json:"RecordCount_INV"`
	CreationDatetime string `json:"CreationDatetime"`
	FromDate         string `json:"FromDate"`
	ToDate           string `json:"ToDate"`
}

type ErrorFileDetail struct {
	FilePath         string `json:"FilePath"`
	StockistCode     string `json:"StockistCode"`
	CreationDatetime string `json:"CreationDatetime"`
	Reson            string `json:"Reson"`
}

// type FileStore struct {
// 	FilePath       string `json:"FilePath"`
// 	StoredDatetime string `json:"StoredDatetime"`
// 	Status         string `json:"Status"`
// 	BucketName     string `json:"BucketName"`
// }

//FileDetails insert file details
func (f *FileDetail) FileDetails(FilePath string, DistributorCode string, SS int, BT int, INV int, Processtime int64, FromDate string, ToDate string, TableName string) *FileDetail {
	f.FilePath = FilePath
	f.StockistCode = DistributorCode
	f.RecordCount_SS = SS
	f.RecordCount_BT = BT
	f.RecordCount_INV = INV
	f.FileProcessTime = Processtime
	f.CreationDatetime = time.Now().In(ConvertUTCtoIST()).Format("2006-01-02 15:04:05")
	f.FromDate = FromDate
	f.ToDate = ToDate
	testinter := f
	err = InserttoBigquery(testinter, TableName)
	if err != nil {
		log.Printf("Error while generating JSON file : %v\n", err)
	}
	//UpdateRecordonBigquery(FilePath, headers.FilePorted)
	return f
}

func (f *ErrorFileDetail) ErrorFileDetails(FilePath string, ErrorMessage string, TableName string, g GcsFile) *ErrorFileDetail {
	g.GcsClient.MoveObject(g.FileName, g.FileName, "awacs-ede1-error")
	//log.Printf("File moved to error bucket: %v", g.FilePath)

	f.FilePath = FilePath
	//f.StockistCode = DistributorCode
	f.Reson = ErrorMessage
	f.CreationDatetime = time.Now().In(ConvertUTCtoIST()).Format("2006-01-02 15:04:05")
	testinter := f
	err = InserttoBigquery(testinter, TableName)
	if err != nil {
		log.Printf("Error while generating JSON file : %v\n", err)
	}

	//UpdateRecordonBigquery(FilePath, headers.FIleError)
	return f
}

// func (s *FileStore) StoreFile(FilePath string, Status string, BucketName string, TableName string) (err error) {

// 	s.FilePath = FilePath
// 	s.BucketName = BucketName
// 	s.Status = Status

// 	s.StoredDatetime = time.Now().In(ConvertUTCtoIST()).Format("2006-01-02 15:04:05")
// 	testinter := s
// 	err = GenerateJsonFile(testinter, TableName)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
