# Databricks notebook source

from pyspark.sql import functions as F

catalog = "workspace"

bronze = spark.table(
    f"{catalog}.default.bronze_online_retail"
)
silver = spark.table(
    f"{catalog}.default.silver_online_retail"
)
daily = spark.table(
    f"{catalog}.default.gold_daily_sales"
)
products = spark.table(
    f"{catalog}.default.gold_product_performance"
)
countries = spark.table(
    f"{catalog}.default.gold_country_performance"
)

print("Tables loaded successfully.")

# COMMAND ----------


tests = []

def record_test(name, passed, details):
    tests.append({
        "test_name": name,
        "status": "PASS" if passed else "FAIL",
        "details": str(details),
    })

# 1. Bronze row count matches the source profile.
bronze_count = bronze.count()
record_test(
    "Bronze source row count",
    bronze_count == 1_067_371,
    f"Observed {bronze_count:,}; expected 1,067,371",
)

# 2. Silver preserves every Bronze row.
silver_count = silver.count()
record_test(
    "Silver preserves source row count",
    silver_count == bronze_count,
    f"Bronze={bronze_count:,}; Silver={silver_count:,}",
)

# 3. Required transaction identifiers are populated.
missing_invoices = silver.filter(
    F.col("invoice_id").isNull()
).count()
record_test(
    "Invoice IDs are non-null",
    missing_invoices == 0,
    f"Missing invoice IDs: {missing_invoices:,}",
)

# 4. Quality flags agree with the actual values.
incorrect_flags = silver.filter(
    (F.col("is_negative_quantity") != (F.col("quantity") < 0))
    | (F.col("is_cancellation_invoice")
       != F.upper(F.col("invoice_id")).startswith("C"))
    | (F.col("is_missing_customer_id")
       != F.col("customer_id").isNull())
).count()

record_test(
    "Quality flags match source fields",
    incorrect_flags == 0,
    f"Rows with inconsistent flags: {incorrect_flags:,}",
)

# 5. Gold daily row counts reconcile to Silver.
daily_line_count = daily.agg(
    F.sum("transaction_line_count")
).first()[0]

record_test(
    "Gold daily row-count reconciliation",
    daily_line_count == silver_count,
    f"Daily lines={daily_line_count:,}; Silver={silver_count:,}",
)

# 6. Gold table grains are unique.
daily_duplicate_dates = (
    daily.groupBy("transaction_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

record_test(
    "Daily table has one row per date",
    daily_duplicate_dates == 0,
    f"Duplicate dates: {daily_duplicate_dates}",
)

product_duplicates = (
    products.groupBy("stock_code")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

record_test(
    "Product table has one row per stock code",
    product_duplicates == 0,
    f"Duplicate product keys: {product_duplicates}",
)

country_duplicates = (
    countries.groupBy("country")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

record_test(
    "Country table has one row per country key",
    country_duplicates == 0,
    f"Duplicate country keys: {country_duplicates}",
)

# Show the complete test report.
test_report = spark.createDataFrame(tests)
display(test_report)

failed = sum(1 for test in tests if test["status"] == "FAIL")
print(f"Tests passed: {len(tests) - failed}/{len(tests)}")

if failed:
    raise AssertionError(
        f"{failed} data-quality test(s) failed. Investigate before proceeding."
    )