# Databricks notebook source
from pyspark.sql import functions as F

catalog = "workspace"

daily = spark.table(f"{catalog}.default.gold_daily_sales")
products = spark.table(f"{catalog}.default.gold_product_performance")
countries = spark.table(f"{catalog}.default.gold_country_performance")
rfm = spark.table(f"{catalog}.default.gold_customer_rfm")

print("Business insight tables loaded successfully.")

# COMMAND ----------

silver = spark.table(f"{catalog}.default.silver_online_retail")

kpis = silver.agg(
    F.count("*").alias("source_line_count"),
    F.countDistinct("invoice_id").alias("distinct_invoice_count"),
    F.sum(
        F.when(F.col("quantity") > 0, F.col("line_amount"))
         .otherwise(F.lit(0))
    ).alias("positive_quantity_line_value"),
    F.sum(
        F.when(F.col("quantity") < 0, -F.col("line_amount"))
         .otherwise(F.lit(0))
    ).alias("negative_quantity_return_value"),
    F.sum(F.col("is_cancellation_invoice").cast("long"))
     .alias("cancellation_invoice_lines"),
    F.sum(F.col("is_duplicate_group").cast("long"))
     .alias("rows_in_duplicate_groups"),
    F.countDistinct(
        F.when(F.col("customer_id").isNotNull(), F.col("customer_id"))
    ).alias("customers_with_ids")
)

display(kpis)

# COMMAND ----------

daily_trend = (
    daily
    .select(
        "transaction_date",
        "positive_quantity_sales_value",
        "negative_quantity_return_value",
        "distinct_invoice_count"
    )
    .orderBy("transaction_date")
)

display(daily_trend)

# COMMAND ----------

top_products = (
    products
    .orderBy(F.desc("positive_quantity_sales_value"))
    .select(
        "stock_code",
        "product_description",
        "distinct_invoice_count",
        "positive_quantity_sales_value",
        "negative_quantity_return_value",
        "net_quantity"
    )
    .limit(10)
)

display(top_products)

# COMMAND ----------

top_countries = (
    countries
    .orderBy(F.desc("positive_quantity_sales_value"))
    .select(
        "country",
        "distinct_invoice_count",
        "positive_quantity_sales_value",
        "negative_quantity_return_value",
        "identified_customer_count"
    )
    .limit(10)
)

display(top_countries)

# COMMAND ----------

segment_summary = (
    rfm
    .groupBy("customer_segment")
    .agg(
        F.count("*").alias("customer_count"),
        F.round(F.avg("recency_days"), 1).alias("avg_recency_days"),
        F.round(F.avg("frequency"), 1).alias("avg_invoice_frequency"),
        F.round(F.avg("monetary_value"), 2).alias("avg_customer_line_value")
    )
    .orderBy(F.desc("customer_count"))
)

display(segment_summary)