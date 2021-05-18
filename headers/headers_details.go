package headers

const Stockist_Code, From_Date, To_Date = 2, 3, 4

const (
	Transaction_row int = iota
	Sr_No
	Company_code
	Company_name
	Item_code
	Item_name
	PACK
	UPC
	PTR
	MRP
	Opening_stock
	Sales_Qty
	Bonus_qty
	Sales_Return
	Expiry_In
	Discount_percentage
	Discount_amount
	Sale_tax
	Purchases_Reciepts
	Purchase_return
	Expiry_out
	Adjustments
	Closing_Stock
	InstaSales
	OpenVal
	PurchaseVal
	SalesVal
	CloseVal
)

const (
	H2_Transaction_row int = iota
	H2_Sr_No
	H2_Item_name
	H2_PACK
	H2_UPC
	H2_BatchNumber
	H2_ExpiryDate
	H2_Closing_Stock
)

const (
	H3_Transaction_row int = iota
	H3_Sr_No
	H3_Company_code
	H3_Company_name
	H3_Invoice_Number
	H3_Invoice_Date
	H3_Invoice_amount
)

const (
	ProjectID       string = "awacs-dev"
	DatasetID       string = "ede_raw_data"
	Filename        string = "/tmp/"
	FileTypePTS     string = "With_PTS"
	FileType        string = "Without_PTS"
	FileTypewithPTR string = "With_PTR"
	FileTypePTR     string = "Without_PTR"
	DurationMTD     string = "MTD"
	DurationMonthly string = "Monthly"
)

const (
	Csv_Transaction_row int = iota
	Csv_Sr_No
	Csv_Company
	Csv_Company_Code
	Csv_Uniform_Pdt_Code
	Csv_Stkt_Product_Code
	Csv_Product_Name
	Csv_Pack
	Csv_Opening_Qty
	Csv_Receipts_Qty
	Csv_Sales_Qty
	Csv_Sales_Ret_Qty
	Csv_Purch_Ret_Qty
	Csv_Adjustments_Qty
	Csv_ClosingQty
)

const PTS, Csv_PTR = 8, 8

const (
	Stock_and_Sales      = "stock_and_sales"
	Batch_details        = "batch_details"
	Invoice_details      = "invoice_details"
	File_details         = "file_details"
	Stock_and_Sales_Dist = "stock_and_sale_dist"
)

const (
	ItemCode int = iota
	ItemName
	CompanyCode
	CompanyName
	SalesQty
	BonusQty
	DiscountPer
	DiscountAmount
	SprPTR
	StaxPerc
	SretQty
	PackSize
	OpeningStock
	ClosingStock
	StockistCode
	FromDate
	ToDate
)

const (
	Companyname int = iota
	ProductName
	OpeingUnits
	PurchaseUnits
	PurchaseFree
	PurchaseReturn
	SalesUnits
	SalesFree
	SalesReturn
	ClosingUnits
	Stockistcode
	Fromdate
	Todate
	StandardPTR
)

const (
	DistName int = iota
	CityName
	StateName
	DFromDate
	DToDate
	Stockist
)
