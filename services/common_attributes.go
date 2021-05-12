package services

import "time"

var (
	FromDate     *time.Time
	ToDate       *time.Time
	ExpiryDate   *time.Time
	StockistCode = ""
	TableId      map[int]string
)

func Init() {
	TableId = make(map[int]string)
	TableId[1] = "stock_and_sales"
	TableId[2] = "batch_details"
	TableId[3] = "invoice_details"
	TableId[4] = "file_details"
}
