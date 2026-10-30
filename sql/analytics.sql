SELECT
    transaction_date,
    positive_quantity_sales_value,
    negative_quantity_return_value
FROM workspace.default.gold_daily_sales
ORDER BY transaction_date;


SELECT
    country,
    positive_quantity_sales_value,
    distinct_invoice_count
FROM workspace.default.gold_country_performance
ORDER BY positive_quantity_sales_value DESC
LIMIT 10;


SELECT
    stock_code,
    product_description,
    positive_quantity_sales_value
FROM workspace.default.gold_product_performance
ORDER BY positive_quantity_sales_value DESC
LIMIT 10;


SELECT
    customer_segment,
    COUNT(*) AS customer_count
FROM workspace.default.gold_customer_rfm
GROUP BY customer_segment
ORDER BY customer_count DESC;

SELECT
    COUNT(*) AS source_line_count,
    SUM(CASE WHEN is_cancellation_invoice
             THEN 1 ELSE 0 END) AS cancellation_lines,
    SUM(CASE WHEN is_duplicate_group
             THEN 1 ELSE 0 END) AS rows_in_duplicate_groups,
    SUM(CASE WHEN customer_id IS NULL
             THEN 1 ELSE 0 END) AS missing_customer_rows
FROM workspace.default.silver_online_retail;

SELECT
    COUNT(*) AS source_row_count
FROM workspace.default.silver_online_retail;

SELECT
    COUNT(DISTINCT invoice_id) AS invoice_count
FROM workspace.default.silver_online_retail;

SELECT
    COUNT(DISTINCT customer_id) AS customer_count
FROM workspace.default.gold_customer_rfm;