# Databricks notebook source

from pyspark.sql import functions as F

catalog = "workspace"
silver = spark.table(f"{catalog}.default.silver_online_retail")

special_codes = ["M", "DOT", "POST", "23843"]

display(
    silver
    .filter(F.col("stock_code").isin(special_codes))
    .groupBy("stock_code", "description")
    .agg(
        F.count("*").alias("line_count"),
        F.countDistinct("invoice_id").alias("invoice_count"),
        F.sum(
            F.when(F.col("quantity") > 0, F.col("line_amount"))
             .otherwise(F.lit(0))
        ).alias("positive_quantity_value"),
        F.sum(
            F.when(F.col("quantity") < 0, F.abs(F.col("line_amount")))
             .otherwise(F.lit(0))
        ).alias("negative_quantity_value"),
        F.sum("quantity").alias("net_quantity"),
        F.sum(F.col("is_cancellation_invoice").cast("long"))
            .alias("cancellation_lines"),
    )
    .orderBy("stock_code", "description")
)

# COMMAND ----------


business_columns = [
    "invoice_id",
    "stock_code",
    "description",
    "quantity",
    "invoice_timestamp",
    "unit_price",
    "customer_id",
    "country",
]

# Baseline: every source row.
baseline = silver.agg(
    F.count("*").alias("row_count"),
    F.sum(
        F.when(F.col("quantity") > 0, F.col("line_amount"))
         .otherwise(F.lit(0))
    ).alias("positive_quantity_value"),
    F.sum(
        F.when(F.col("quantity") < 0, F.abs(F.col("line_amount")))
         .otherwise(F.lit(0))
    ).alias("negative_quantity_value"),
)

# Exploratory scenario: one row per exact business-field combination.
# This is NOT yet our approved deduplication policy.
deduplicated_scenario = silver.dropDuplicates(business_columns)

comparison = deduplicated_scenario.agg(
    F.count("*").alias("rows_after_exact_deduplication"),
    F.sum(
        F.when(F.col("quantity") > 0, F.col("line_amount"))
         .otherwise(F.lit(0))
    ).alias("positive_quantity_value"),
    F.sum(
        F.when(F.col("quantity") < 0, F.abs(F.col("line_amount")))
         .otherwise(F.lit(0))
    ).alias("negative_quantity_value"),
)

print("All source rows:")
baseline.show(truncate=False)

print("Exploratory exact-deduplication scenario:")
comparison.show(truncate=False)

# COMMAND ----------


display(
    silver
    .groupBy(
        "is_cancellation_invoice",
        "is_negative_quantity"
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum("line_amount").alias("net_signed_line_value"),
    )
    .orderBy(
        "is_cancellation_invoice",
        "is_negative_quantity"
    )
)