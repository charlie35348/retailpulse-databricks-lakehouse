# RetailPulse – Architecture

## Overview

RetailPulse is an end-to-end retail data engineering project built with Databricks, PySpark, Spark SQL and Delta Lake. It uses the UCI Online Retail II dataset to demonstrate a Bronze–Silver–Gold lakehouse architecture, data-quality validation, customer segmentation and business analytics.

## Architecture flow

```text
UCI Online Retail II dataset
            |
            v
   Source CSV files
            |
            v
   Databricks Volume
            |
            v
   Bronze – Raw ingestion
            |
            v
   Silver – Standardisation
   and data-quality flags
            |
            v
   Gold – Analytics tables
            |
            +----------------------+
            |                      |
            v                      v
   Daily / product /       Customer RFM
   country performance     segmentation
            |                      |
            +----------+-----------+
                       |
                       v
             Databricks SQL
                Dashboard
```

## Data layers

### Bronze

- Reads the source CSV files from a Unity Catalog Volume.
- Applies an explicit Spark schema.
- Adds ingestion metadata and source-file information.
- Stores the ingested records in a Delta table.

### Silver

- Standardises and cleans selected fields.
- Handles blank descriptions and missing customer identifiers.
- Creates quality indicators for cancellation invoices, negative quantities, zero or negative prices, missing values and duplicate groups.
- Calculates line-level values.

### Gold

Creates analytical tables for:
- Daily sales and return analysis.
- Product performance.
- Country performance.
- Customer RFM segmentation.

## Data quality

The project includes automated validation notebooks for the core lakehouse tables and customer RFM output. The recorded validation runs completed with 8/8 core checks and 9/9 RFM checks passing.

## Orchestration

A Databricks Lakeflow Job orchestrates the main workflow:

1. Bronze ingestion
2. Silver transformations
3. Gold analytics
4. Core data-quality tests
5. Customer RFM segmentation
6. RFM data-quality tests

The current workflow uses overwrite-based full refreshes. It is a portfolio demonstration and should not be interpreted as an incremental production ingestion system.

## Analytics and reporting

A Databricks SQL dashboard presents daily trends, product and country performance, customer segments and data-quality indicators using the Gold and RFM tables.

## Technology

- Databricks
- Apache Spark / PySpark
- Spark SQL
- Delta Lake
- Unity Catalog Volumes
- Lakeflow Jobs
- Databricks SQL
- Python
- Git and GitHub