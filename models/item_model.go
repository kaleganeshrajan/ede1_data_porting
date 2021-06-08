package models

type Item struct {
	Item_code           string  `json:"ItemCode"`
	Item_name           string  `json:"ItemName"`
	Pack                string  `json:"Pack"`
	UPC                 string  `json:"UPC"`
	PTR                 float64 `json:"PTR"`
	PTS                 float64 `json:"PTS"`
	MRP                 float64 `json:"MRP"`
	Opening_stock       float64 `json:"OpeningStock"`
	Sales_qty           float64 `json:"SalesQty"`
	Bonus_qty           float64 `json:"BonusQty"`
	Sales_return        float64 `json:"SalesReturn"`
	Expiry_in           float64 `json:"ExpiryIn"`
	Discount_percentage float64 `json:"DiscountPercentage"`
	Discount_amount     float64 `json:"DiscountAmount"`
	Sale_tax            float64 `json:"SaleTax"`
	Purchases_Reciepts  float64 `json:"PurchasesReciept"`
	Purchase_return     float64 `json:"PurchaseReturn"`
	Expiry_out          float64 `json:"ExpiryOut"`
	Adjustments         float64 `json:"Adjustments"`
	Closing_Stock       float64 `json:"ClosingStock"`
	UniformPdtCode      string  `json:"UniformPdtCode"`
	InstaSales          float64 `json:"InstaSales"`
	OpenVal             float64 `json:"OpenVal"`
	PurchaseVal         float64 `json:"PurchaseVal"`
	SalesVal            float64 `json:"SalesVal"`
	CloseVal            float64 `json:"CloseVal"`
	PurchaseFree        float64 `json:"PurchaseFree"`
	SalesFree           float64 `json:"SalesFree"`
}

type ItemBatch struct {
	Item_name    string `json:"ItemName"`
	Pack         string `json:"Pack"`
	UPC          string `json:"UPC"`
	Batch_number string `json:"BatchNumber"`
	Expiry_date  string `json:"ExpiryDate"`
	Closing_Qty  string `json:"ClosingQuantity"`
}
