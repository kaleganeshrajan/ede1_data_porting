package models

type Item struct {
	Item_code           string `json:"ItemCode"`
	Item_name           string `json:"ItemName"`
	Pack                string `json:"Pack"`
	UPC                 string `json:"UPC"`
	PTR                 string `json:"PTR"`
	PTS                 string `json:"PTS"`
	MRP                 string `json:"MRP"`
	Opening_stock       string `json:"OpeningStock"`
	Sales_qty           string `json:"SalesQty"`
	Bonus_qty           string `json:"BonusQty"`
	Sales_return        string `json:"SalesReturn"`
	Expiry_in           string `json:"ExpiryIn"`
	Discount_percentage string `json:"DiscountPercentage"`
	Discount_amount     string `json:"DiscountAmount"`
	Sale_tax            string `json:"SaleTax"`
	Purchases_Reciepts  string `json:"PurchasesReciept"`
	Purchase_return     string `json:"PurchaseReturn"`
	Expiry_out          string `json:"ExpiryOut"`
	Adjustments         string `json:"Adjustments"`
	Closing_Stock       string `json:"ClosingStock"`
	UniformPdtCode      string `json:"UniformPdtCode"`
	InstaSales          string `json:"InstaSales"`
	OpenVal             string `json:"OpenVal"`
	PurchaseVal         string `json:"PurchaseVal"`
	SalesVal            string `json:"SalesVal"`
	CloseVal            string `json:"CloseVal"`
	PurchaseFree        string `json:"PurchaseFree"`
	SalesFree           string `json:"SalesFree"`
}

type ItemBatch struct {
	Item_name    string `json:"ItemName"`
	Pack         string `json:"Pack"`
	UPC          string `json:"UPC"`
	Batch_number string `json:"BatchNumber"`
	Expiry_date  string `json:"ExpiryDate"`
	Closing_Qty  string `json:"ClosingQuantity"`
}
