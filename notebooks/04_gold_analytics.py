# Databricks notebook source

from pyspark.sql import functions as F

catalog = "workspace"

silver_table = f"{catalog}.default.silver_online_retail"

gold_daily_table = f"{catalog}.default.gold_daily_sales"
gold_product_table = f"{catalog}.default.gold_product_performance"
gold_country_table = f"{catalog}.default.gold_country_performance"

silver = spark.table(silver_table)

print(f"Silver rows available: {silver.count():,}")

# COMMAND ----------


daily_sales = (
    silver
    .withColumn(
        "transaction_date",
        F.to_date("invoice_timestamp")
    )
    .groupBy("transaction_date")
    .agg(
        F.count("*").alias("transaction_line_count"),

        F.countDistinct("invoice_id")
            .alias("distinct_invoice_count"),

        F.sum(
            F.when(
                F.col("quantity") > 0,
                F.col("line_amount")
            ).otherwise(F.lit(0))
        ).alias("positive_quantity_sales_value"),

        F.sum(
            F.when(
                F.col("quantity") < 0,
                F.abs(F.col("line_amount"))
            ).otherwise(F.lit(0))
        ).alias("negative_quantity_return_value"),

        F.sum("line_amount")
            .alias("net_signed_line_value"),

        F.sum(
            F.col("is_cancellation_invoice").cast("long")
        ).alias("cancellation_invoice_line_count"),

        F.sum(
            F.col("is_duplicate_group").cast("long")
        ).alias("rows_in_duplicate_groups")
    )
    
)

(
    daily_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_daily_table)
)

display(
    spark.table(gold_daily_table)
    .orderBy("transaction_date")
    .limit(20)
)

# COMMAND ----------


product_performance = (
    silver
    .groupBy("stock_code")
    .agg(
        F.first("description", ignorenulls=True)
            .alias("product_description"),

        F.count("*").alias("transaction_line_count"),

        F.sum(
            F.when(
                F.col("quantity") > 0,
                F.col("line_amount")
            ).otherwise(F.lit(0))
        ).alias("positive_quantity_sales_value"),

        F.sum(
            F.when(
                F.col("quantity") < 0,
                F.abs(F.col("line_amount"))
            ).otherwise(F.lit(0))
        ).alias("negative_quantity_return_value"),

        F.sum("quantity").alias("net_quantity"),

        F.countDistinct("invoice_id")
            .alias("distinct_invoice_count")
    )
    
)

(
    product_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_product_table)
)

display(
    spark.table(gold_product_table)
    .orderBy(F.desc("positive_quantity_sales_value"))
    .limit(20)
)

# COMMAND ----------


country_performance = (
    silver
    .groupBy("country")
    .agg(
        F.count("*").alias("transaction_line_count"),

        F.countDistinct("invoice_id")
            .alias("distinct_invoice_count"),

        F.sum(
            F.when(
                F.col("quantity") > 0,
                F.col("line_amount")
            ).otherwise(F.lit(0))
        ).alias("positive_quantity_sales_value"),

        F.sum(
            F.when(
                F.col("quantity") < 0,
                F.abs(F.col("line_amount"))
            ).otherwise(F.lit(0))
        ).alias("negative_quantity_return_value"),

        F.countDistinct("customer_id")
            .alias("identified_customer_count")
    )
    
)

(
    country_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_country_table)
)

display(
    spark.table(gold_country_table)
    .orderBy(F.desc("positive_quantity_sales_value"))
)

# COMMAND ----------


gold_daily = spark.table(gold_daily_table)
gold_products = spark.table(gold_product_table)
gold_countries = spark.table(gold_country_table)

print("=== GOLD TABLE VALIDATION ===")

print(f"Daily sales rows: {gold_daily.count():,}")
print(f"Product rows: {gold_products.count():,}")
print(f"Country rows: {gold_countries.count():,}")

print("\n=== DAILY METRIC RECONCILIATION ===")

daily_totals = gold_daily.agg(
    F.sum("transaction_line_count").alias("source_lines"),
    F.sum("positive_quantity_sales_value").alias("positive_sales_value"),
    F.sum("negative_quantity_return_value").alias("negative_quantity_return_value"),
    F.sum("cancellation_invoice_line_count").alias("cancellation_lines"),
    F.sum("rows_in_duplicate_groups").alias("duplicate_group_rows")
)

daily_totals.show(truncate=False)

print("\n=== TOP 10 COUNTRIES ===")

display(
    gold_countries
    .orderBy(F.desc("positive_quantity_sales_value"))
    .limit(10)
)

print("\n=== TOP 10 PRODUCTS ===")

display(
    gold_products
    .orderBy(F.desc("positive_quantity_sales_value"))
    .limit(10)
)