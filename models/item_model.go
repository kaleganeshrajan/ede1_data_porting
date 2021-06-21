package models

type Item struct {
	ItemCode           string  `json:"ItemCode"`
	ItemName           string  `json:"ItemName"`
	SearchString        string  `json:"SearchString"`
	Pack                string  `json:"Pack"`
	UPC                 string  `json:"UPC"`
	PTR                 float64 `json:"PTR"`
	PTS                 float64 `json:"PTS"`
	MRP                 float64 `json:"MRP"`
	OpeningStock       float64 `json:"OpeningStock"`
	SalesQty           float64 `json:"SalesQty"`
	BonusQty           float64 `json:"BonusQty"`
	SalesReturn        float64 `json:"SalesReturn"`
	ExpiryIn           float64 `json:"ExpiryIn"`
	DiscountPercentage float64 `json:"DiscountPercentage"`
	DiscountAmount     float64 `json:"DiscountAmount"`
	SaleTax            float64 `json:"SaleTax"`
	PurchasesReciept  float64 `json:"PurchasesReciept"`
	PurchaseReturn     float64 `json:"PurchaseReturn"`
	ExpiryOut          float64 `json:"ExpiryOut"`
	Adjustments         float64 `json:"Adjustments"`
	ClosingStock       float64 `json:"ClosingStock"`
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
	SearchString string `json:"SearchString"`
	Pack         string `json:"Pack"`
	UPC          string `json:"UPC"`
	Batch_number string `json:"BatchNumber"`
	Expiry_date  string `json:"ExpiryDate"`
	Closing_Qty  string `json:"ClosingQuantity"`
}
