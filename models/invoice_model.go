package models

type Invoice struct {
	Invoice_Number string `json:"InvoiceNumber"`
	Invoice_Date   string `json:"InvoiceDate"`
	Invoice_Amount string `json:"InvoiceAmount"`
}
