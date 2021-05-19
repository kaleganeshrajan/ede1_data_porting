package models

type Record struct {
	FilePath         string    `json:"FilePath"`
	DistributorCode  string    `json:"DistributorCode"`
	ToDate           string    `json:"ToDate"`
	FromDate         string    `json:"FromDate"`
	FileType         string    `json:"FileType"`
	Duration         string    `json:"Duration"`
	Key              string    `json:"Key"`
	CreationDatetime string    `json:"CreationDatetime"`
	Companies        []Company `json:"Companies"`
}

type RecordInvoice struct {
	FilePath         string           `json:"FilePath"`
	DistributorCode  string           `json:"DistributorCode"`
	ToDate           string           `json:"ToDate"`
	FromDate         string           `json:"FromDate"`
	FileType         string           `json:"FileType"`
	Duration         string           `json:"Duration"`
	CreationDatetime string           `json:"CreationDatetime"`
	Companies        []CompanyInvoice `json:"Companies"`
}

type RecordBatch struct {
	FilePath         string      `json:"FilePath"`
	DistributorCode  string      `json:"DistributorCode"`
	ToDate           string      `json:"ToDate"`
	FromDate         string      `json:"FromDate"`
	FileType         string      `json:"FileType"`
	Duration         string      `json:"Duration"`
	CreationDatetime string      `json:"CreationDatetime"`
	Batches          []ItemBatch `json:"Batches"`
}

type RecordDist struct {
	FilePath         string `json:"FilePath"`
	DistributorCode  string `json:"DistributorCode"`
	ToDate           string `json:"ToDate"`
	FromDate         string `json:"FromDate"`
	Duration         string `json:"Duration"`
	Key              string `json:"Key"`
	DistName         string `json:"DistName"`
	StateName        string `json:"StateName"`
	CityName         string `json:"CityName"`
	CreationDatetime string `json:"CreationDatetime"`
}
