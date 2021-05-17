package models

type FileDetails struct {
	FilePath         string `json:"FilePath"`
	StockistCode     string `json:"StockistCode"`
	FileProcessTime  int64  `json:"FileProcessTime"`
	RecordCount_SS   int    `json:"RecordCount_SS"`
	RecordCount_BT   int    `json:"RecordCount_BT"`
	RecordCount_INV  int    `json:"RecordCount_INV"`
	CreationDatetime string `json:"CreationDatetime"`
}
