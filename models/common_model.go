package models

import "time"

//Common attribute
type Common struct {
	FromDate     *time.Time
	ToDate       *time.Time
	ExpiryDate   *time.Time
	StockistCode string
}
