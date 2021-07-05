package models

type Invoice struct {
	InvoiceNumber string `json:"InvoiceNumber"`
	InvoiceDate   string `json:"InvoiceDate"`
	InvoiceAmount float64 `json:"InvoiceAmount"`
}
