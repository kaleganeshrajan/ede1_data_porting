package models

type Record struct {
	FilePath        string    `json:"FilePath"`
	DistributorCode string    `json:"DistributorCode"`
	ToDate          string    `json:"ToDate"`
	FromDate        string    `json:"FromDate"`
	FileType        string    `json:"FileType"`
	Duration        string    `json:"Duration"`
	Key             string    `json:"Key"`
	Companies       []Company `json:"Companies"`
}

type RecordInvoice struct {
	FilePath        string           `json:"FilePath"`
	DistributorCode string           `json:"DistributorCode"`
	ToDate          string           `json:"ToDate"`
	FromDate        string           `json:"FromDate"`
	FileType        string           `json:"FileType"`
	Duration        string           `json:"Duration"`
	Companies       []CompanyInvoice `json:"Companies"`
}

type RecordBatch struct {
	FilePath        string      `json:"FilePath"`
	DistributorCode string      `json:"DistributorCode"`
	ToDate          string      `json:"ToDate"`
	FromDate        string      `json:"FromDate"`
	FileType        string      `json:"FileType"`
	Duration        string      `json:"Duration"`
	Batches         []ItemBatch `json:"Batches"`
}
