package models

type Company struct {
	CompanyName string    `json:"CompanyName"`
	CompanyCode string    `json:"CompanyCode"`
	Items       []Item    `json:"Items"`
}

type CompanyInvoice struct {
	CompanyName string    `json:"CompanyName"`
	CompanyCode string    `json:"CompanyCode"`
	Invoices     []Invoice `json:"Invoices"`
}
