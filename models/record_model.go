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

type SaleDist struct {
	ITEM_CODE string
	ITEM_NAME string
	MANU_CODE string
	MANU_NAME string
	SALE_QTY  string
	BON_QTY   string
	DIS_PERC  string
	DIS_AMT   string
	SPR_PTR   string
	STAX_PERC string
	SRET_QTY  string
	PACK_SIZE string
	ACODE     string
	OP_BAL    string
	CL_BAL    string
	FROM_DATE string
	TO_DATE   string
}
