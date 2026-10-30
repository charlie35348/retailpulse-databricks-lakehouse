# RetailPulse – Data Dictionary

## Source dataset

**Dataset:** Online Retail II  
**Publisher:** UCI Machine Learning Repository  
**Source:** https://archive.ics.uci.edu/dataset/502/online+retail+ii?utm_source

The dataset contains online retail transactions from a UK-based, non-store online retailer. It includes two periods of transaction records.

## Source fields

| Source field | Description |
|---|---|
| Invoice | Invoice number identifying a transaction. Invoice numbers beginning with `C` indicate cancellations in the source data. |
| StockCode | Product or item code. |
| Description | Product description. |
| Quantity | Number of units recorded on the transaction line. Negative values can represent returns or adjustments. |
| InvoiceDate | Date and time of the transaction. |
| Price | Unit price recorded in the source data. |
| Customer ID | Customer identifier; some records do not contain one. |
| Country | Customer's country. |

## Project processing

### Bronze table

`bronze_online_retail`

Contains the ingested source records, standardised column names, source-file information and ingestion metadata.

### Silver table

`silver_online_retail`

Contains cleaned and standardised records, quality flags and calculated line-level values.

Important derived fields include:
- `is_cancellation_invoice`
- `is_negative_quantity`
- `is_zero_quantity`
- `is_zero_price`
- `is_negative_price`
- `has_missing_customer`
- `has_missing_description`
- `line_amount`
- Duplicate-group indicators

### Gold tables

| Table | Purpose |
|---|---|
| `gold_daily_sales` | Daily transaction, invoice, sales and return metrics. |
| `gold_product_performance` | Product-level performance analysis. |
| `gold_country_performance` | Country-level performance analysis. |
| `gold_customer_rfm` | Customer recency, frequency, monetary metrics and segment labels. |

## Interpretation notes

- The source contains missing customer identifiers and descriptions.
- Negative quantities and cancellation-style invoice identifiers are retained and analysed rather than automatically discarded.
- Duplicate-group flags identify records for investigation; they do not mean that all such records have been removed.
- Monetary figures are derived from the source transaction lines and should not automatically be interpreted as audited accounting revenue.
- RFM recency is calculated relative to the dataset's maximum transaction date, not the current date.
- Customer RFM analysis excludes records without an identifiable customer ID.