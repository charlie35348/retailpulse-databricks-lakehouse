# Databricks notebook source
from pyspark.sql import functions as F

catalog = "workspace"

rfm_table = f"{catalog}.default.gold_customer_rfm"
rfm = spark.table(rfm_table)

print(f"RFM rows: {rfm.count():,}")
rfm.printSchema()

# COMMAND ----------

# Calculate the metrics needed for validation
metrics = rfm.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.sum(F.col("customer_id").isNull().cast("long")).alias("null_customer_ids"),
    F.sum(F.col("recency_days").isNull().cast("long")).alias("null_recency"),
    F.sum(F.col("frequency").isNull().cast("long")).alias("null_frequency"),
    F.sum(F.col("monetary_value").isNull().cast("long")).alias("null_monetary"),
    F.sum((F.col("recency_days") < 0).cast("long")).alias("negative_recency"),
    F.sum((F.col("frequency") < 1).cast("long")).alias("invalid_frequency"),
    F.sum(
        (F.col("qualifying_line_count") < F.col("frequency")).cast("long")
    ).alias("invalid_line_count"),
).first()

# Count records that violate the scoring rules
invalid_scores = rfm.filter(
    ~F.col("recency_score").between(1, 4)
    | ~F.col("frequency_score").between(1, 4)
    | ~F.col("monetary_score").between(1, 4)
).count()

# Rebuild the expected RFM score from its three component scores
invalid_rfm_strings = rfm.filter(
    F.col("rfm_score") != F.concat(
        F.col("recency_score").cast("string"),
        F.col("frequency_score").cast("string"),
        F.col("monetary_score").cast("string")
    )
).count()

allowed_segments = [
    "High Value - Active",
    "Loyal - Active",
    "Needs Re-engagement",
    "Regular"
]

invalid_segments = rfm.filter(
    F.col("customer_segment").isNull()
    | ~F.col("customer_segment").isin(allowed_segments)
).count()

# Each test returns a name, pass/fail status and diagnostic detail
tests = [
    (
        "One row per customer",
        metrics["row_count"] == metrics["unique_customers"],
        f"rows={metrics['row_count']:,}, unique={metrics['unique_customers']:,}"
    ),
    (
        "No null customer IDs",
        metrics["null_customer_ids"] == 0,
        f"null IDs={metrics['null_customer_ids']}"
    ),
    (
        "No null RFM metrics",
        metrics["null_recency"] == 0
        and metrics["null_frequency"] == 0
        and metrics["null_monetary"] == 0,
        "Check recency, frequency and monetary null counts"
    ),
    (
        "Recency is non-negative",
        metrics["negative_recency"] == 0,
        f"negative recency={metrics['negative_recency']}"
    ),
    (
        "Frequency is at least one",
        metrics["invalid_frequency"] == 0,
        f"invalid frequency={metrics['invalid_frequency']}"
    ),
    (
        "Line count is consistent with frequency",
        metrics["invalid_line_count"] == 0,
        f"invalid records={metrics['invalid_line_count']}"
    ),
    (
        "RFM component scores are between 1 and 4",
        invalid_scores == 0,
        f"invalid score records={invalid_scores}"
    ),
    (
        "RFM string matches component scores",
        invalid_rfm_strings == 0,
        f"mismatches={invalid_rfm_strings}"
    ),
    (
        "Customer segments are valid",
        invalid_segments == 0,
        f"invalid segments={invalid_segments}"
    ),
]

for name, passed, detail in tests:
    print(f"{'PASS' if passed else 'FAIL'} | {name} | {detail}")

passed_count = sum(1 for _, passed, _ in tests if passed)
failed_count = len(tests) - passed_count

print(f"\nRFM quality result: {passed_count}/{len(tests)} passed")

if failed_count:
    raise AssertionError(f"{failed_count} RFM quality test(s) failed.")