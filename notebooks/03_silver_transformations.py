# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "workspace"

bronze_table = f"{catalog}.default.bronze_online_retail"
silver_table = f"{catalog}.default.silver_online_retail"

bronze = spark.table(bronze_table)

silver = (
    bronze
    .withColumn("invoice_id", F.trim(F.col("invoice_id")))
    .withColumn("stock_code", F.trim(F.col("stock_code")))
    .withColumn(
        "description",
        F.when(
            F.trim(F.col("description")) == "",
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("description")))
    )
    .withColumn(
        "customer_id",
        F.when(
            F.trim(F.col("customer_id")) == "",
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("customer_id")))
    )
    .withColumn("country", F.trim(F.col("country")))
)

print(f"Bronze rows: {bronze.count():,}")
print(f"Silver candidate rows: {silver.count():,}")

# COMMAND ----------


silver = (
    silver
    .withColumn(
        "is_cancellation_invoice",
        F.upper(F.col("invoice_id")).startswith("C")
    )
    .withColumn(
        "is_negative_quantity",
        F.col("quantity") < 0
    )
    .withColumn(
        "is_zero_quantity",
        F.col("quantity") == 0
    )
    .withColumn(
        "is_zero_price",
        F.col("unit_price") == 0
    )
    .withColumn(
        "is_negative_price",
        F.col("unit_price") < 0
    )
    .withColumn(
        "is_missing_customer_id",
        F.col("customer_id").isNull()
    )
    .withColumn(
        "is_missing_description",
        F.col("description").isNull()
    )
    .withColumn(
        "line_amount",
        F.col("quantity") * F.col("unit_price")
    )
    .withColumn(
        "transformed_at",
        F.current_timestamp()
    )
)

display(silver.select(
    "invoice_id",
    "stock_code",
    "quantity",
    "unit_price",
    "line_amount",
    "is_cancellation_invoice",
    "is_negative_quantity",
    "is_zero_price",
    "is_missing_customer_id"
).limit(20))

# COMMAND ----------

from pyspark.sql.window import Window
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

duplicate_window = Window.partitionBy(*business_columns)

silver = (
    silver
    .withColumn(
        "duplicate_group_size",
        F.count(F.lit(1)).over(duplicate_window)
    )
    .withColumn(
        "is_duplicate_group",
        F.col("duplicate_group_size") > 1
    )
)

display(
    silver
    .filter(F.col("is_duplicate_group"))
    .select(
        *business_columns,
        "duplicate_group_size",
        "source_file_name"
    )
    .limit(20)
)

# COMMAND ----------


(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Silver table created: {silver_table}")

# COMMAND ----------


silver_check = spark.table(silver_table)

silver_check.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.col("is_duplicate_group").cast("long"))
        .alias("rows_in_duplicate_groups"),
    F.sum(F.col("is_cancellation_invoice").cast("long"))
        .alias("cancellation_invoice_rows"),
    F.sum(F.col("is_negative_quantity").cast("long"))
        .alias("negative_quantity_rows"),
    F.sum(F.col("is_zero_price").cast("long"))
        .alias("zero_price_rows"),
    F.sum(F.col("is_negative_price").cast("long"))
        .alias("negative_price_rows"),
    F.sum(F.col("is_missing_customer_id").cast("long"))
        .alias("missing_customer_ids"),
    F.sum(F.col("is_missing_description").cast("long"))
        .alias("missing_descriptions"),
    F.sum(F.col("line_amount").isNull().cast("long"))
        .alias("null_line_amounts"),
).show(truncate=False)

print("Silver schema:")
silver_check.printSchema()