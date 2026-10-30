# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "workspace"

silver_table = f"{catalog}.default.silver_online_retail"
rfm_table = f"{catalog}.default.gold_customer_rfm"

silver = spark.table(silver_table)

qualifying_transactions = silver.filter(
    F.col("customer_id").isNotNull()
    & (F.trim(F.col("customer_id")) != "")
    & (F.col("quantity") > 0)
    & (~F.col("is_cancellation_invoice"))
    & F.col("invoice_timestamp").isNotNull()
)

print(
    "Qualifying transaction lines:",
    f"{qualifying_transactions.count():,}"
)

print(
    "Identified customers:",
    f"{qualifying_transactions.select('customer_id').distinct().count():,}"
)

# COMMAND ----------


reference_date = silver.agg(
    F.max("invoice_timestamp").alias("latest_date")
).first()["latest_date"].date()

customer_rfm = (
    qualifying_transactions
    .groupBy("customer_id")
    .agg(
        F.max("invoice_timestamp").alias("last_purchase_timestamp"),
        F.countDistinct("invoice_id").alias("frequency"),
        F.sum("line_amount").alias("monetary_value"),
        F.count("*").alias("qualifying_line_count"),
    )
    .withColumn(
        "recency_days",
        F.datediff(
            F.lit(reference_date),
            F.to_date("last_purchase_timestamp")
        )
    )
    .withColumn(
        "monetary_value",
        F.round("monetary_value", 4)
    )
)

print("Reference date:", reference_date)
display(customer_rfm.orderBy("recency_days").limit(10))

# COMMAND ----------


recency_window = Window.orderBy(
    F.col("recency_days").asc(),
    F.col("customer_id").asc()
)

frequency_window = Window.orderBy(
    F.col("frequency").asc(),
    F.col("customer_id").asc()
)

monetary_window = Window.orderBy(
    F.col("monetary_value").asc(),
    F.col("customer_id").asc()
)

customer_rfm = (
    customer_rfm
    .withColumn("recency_bucket", F.ntile(4).over(recency_window))
    .withColumn("frequency_score", F.ntile(4).over(frequency_window))
    .withColumn("monetary_score", F.ntile(4).over(monetary_window))
    .withColumn("recency_score", 5 - F.col("recency_bucket"))
    .withColumn(
        "rfm_score",
        F.concat(
            F.col("recency_score").cast("string"),
            F.col("frequency_score").cast("string"),
            F.col("monetary_score").cast("string")
        )
    )
)

# COMMAND ----------


customer_rfm = customer_rfm.withColumn(
    "customer_segment",
    F.when(
        (F.col("recency_score") >= 3)
        & (F.col("frequency_score") >= 3)
        & (F.col("monetary_score") >= 3),
        "High Value - Active"
    )
    .when(
        (F.col("recency_score") >= 3)
        & (F.col("frequency_score") >= 3),
        "Loyal - Active"
    )
    .when(
        F.col("recency_score") <= 2,
        "Needs Re-engagement"
    )
    .otherwise("Regular")
)

display(
    customer_rfm
    .groupBy("customer_segment")
    .agg(
        F.count("*").alias("customer_count"),
        F.round(F.avg("frequency"), 2).alias("avg_invoice_frequency"),
        F.round(F.avg("monetary_value"), 2).alias("avg_monetary_value"),
        F.round(F.avg("recency_days"), 2).alias("avg_recency_days"),
    )
    .orderBy(F.desc("customer_count"))
)

# COMMAND ----------


(
    customer_rfm.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(rfm_table)
)

rfm_check = spark.table(rfm_table)

print("RFM customer rows:", f"{rfm_check.count():,}")
print(
    "Unique customer IDs:",
    f"{rfm_check.select('customer_id').distinct().count():,}"
)

print("Null or invalid RFM values:")
rfm_check.select(
    F.sum(F.col("recency_days").isNull().cast("long")).alias("null_recency"),
    F.sum(F.col("frequency").isNull().cast("long")).alias("null_frequency"),
    F.sum(F.col("monetary_value").isNull().cast("long")).alias("null_monetary"),
    F.sum((F.col("recency_days") < 0).cast("long")).alias("negative_recency"),
).show()

display(
    rfm_check
    .orderBy(F.desc("monetary_value"))
    .limit(20)
)