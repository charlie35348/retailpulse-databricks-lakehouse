# Databricks notebook source

catalog = "workspace"

volume_path = f"/Volumes/{catalog}/default/retailpulse_raw"

display(dbutils.fs.ls(volume_path))

# COMMAND ----------

catalog = "workspace"

volume_path = f"/Volumes/{catalog}/default/retailpulse_raw"
bronze_table = f"{catalog}.default.bronze_online_retail"

print("Source:", volume_path)
print("Target table:", bronze_table)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType,
)

source_schema = StructType([
    StructField("Invoice", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("Price", DecimalType(18, 4), True),
    StructField("Customer ID", StringType(), True),
    StructField("Country", StringType(), True),
])

print(source_schema.simpleString())

# COMMAND ----------

from pyspark.sql import functions as F

raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .option("mode", "PERMISSIVE")
    .schema(source_schema)
    .load(f"{volume_path}/*.csv")
)

print("Rows read:", f"{raw_df.count():,}")
raw_df.printSchema()

display(raw_df.limit(10))

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn(
        "source_file_name",
        F.regexp_extract(
            F.col("_metadata.file_path"),
            r"([^/]+)$",
            1
        )
    )
    .withColumn("ingested_at", F.current_timestamp())
    .withColumnRenamed("Invoice", "invoice_id")
    .withColumnRenamed("StockCode", "stock_code")
    .withColumnRenamed("Description", "description")
    .withColumnRenamed("Quantity", "quantity")
    .withColumnRenamed("InvoiceDate", "invoice_timestamp")
    .withColumnRenamed("Price", "unit_price")
    .withColumnRenamed("Customer ID", "customer_id")
    .withColumnRenamed("Country", "country")
)

display(bronze_df.limit(10))

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Bronze table created: {bronze_table}")

# COMMAND ----------

bronze_check = spark.table(bronze_table)

print("Bronze row count:", f"{bronze_check.count():,}")

display(
    bronze_check
    .groupBy("source_file_name")
    .agg(F.count("*").alias("row_count"))
    .orderBy("source_file_name")
)

display(
    bronze_check.select(
        F.count("*").alias("total_rows"),
        F.count("invoice_id").alias("non_null_invoices"),
        F.count("invoice_timestamp").alias("non_null_timestamps"),
        F.count("quantity").alias("non_null_quantities"),
        F.count("unit_price").alias("non_null_prices"),
        F.count("customer_id").alias("non_null_customer_ids"),
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table(bronze_table)

# Business columns only: exclude ingestion metadata.
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

# Count exact duplicate rows without deleting anything.
duplicate_groups = (
    bronze
    .groupBy(*business_columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_excess = (
    duplicate_groups
    .select(F.sum(F.col("count") - 1).alias("duplicate_excess"))
    .first()["duplicate_excess"]
    or 0
)

# Measure the quality indicators.
quality_profile = bronze.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.col("description").isNull().cast("long"))
        .alias("missing_descriptions"),
    F.sum(F.col("customer_id").isNull().cast("long"))
        .alias("missing_customer_ids"),
    F.sum((F.col("quantity") < 0).cast("long"))
        .alias("negative_quantities"),
    F.sum((F.col("quantity") == 0).cast("long"))
        .alias("zero_quantities"),
    F.sum((F.col("unit_price") == 0).cast("long"))
        .alias("zero_prices"),
    F.sum((F.col("unit_price") < 0).cast("long"))
        .alias("negative_prices"),
    F.sum(
        F.upper(F.trim(F.col("invoice_id"))).startswith("C").cast("long")
    ).alias("cancellation_style_invoices"),
    F.min("invoice_timestamp").alias("earliest_transaction"),
    F.max("invoice_timestamp").alias("latest_transaction"),
)

print("=== BRONZE QUALITY PROFILE ===")
quality_profile.show(truncate=False)

print(f"Exact duplicate rows beyond the first occurrence: {duplicate_excess:,}")
print(f"Exact duplicate groups: {duplicate_groups.count():,}")