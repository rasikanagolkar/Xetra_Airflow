DROP TABLE IF EXISTS src_stock_data;
DROP TABLE IF EXISTS run_info;

CREATE TABLE IF NOT EXISTS src_stock_data
(
ISIN TEXT NOT NULL
,Mnemonic	TEXT NOT NULL
,SecurityDesc	TEXT NOT NULL
,SecurityType	TEXT NOT NULL
,Currency	TEXT NOT NULL
,SecurityID	TEXT NOT NULL
,Date	TEXT NOT NULL
,Time	TEXT NOT NULL
,StartPrice	TEXT NOT NULL
,MaxPrice	TEXT NOT NULL
,MinPrice	TEXT NOT NULL
,EndPrice	TEXT NOT NULL
,TradedVolume	TEXT NOT NULL
,NumberOfTrades TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_info
(
    source_date Date NULL,
    datetime_of_processing Date NULL
);

