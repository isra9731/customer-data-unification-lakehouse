# Customer Data Unification & Lakehouse Engineering
## Project Overview
This project implements an end-to-end customer data engineering solution using Microsoft Fabric, PySpark, Delta Lake and SQL.
    
The solution follows the Medallion Architecture:
- Bronze: Raw source ingestion and ingestion metadata.
- Silver: Data cleansing, standardisation, validation,
  deduplication and entity resolution.
- Gold: Curated customer data, transaction analytics,
  Customer 360 and business-ready datasets.

## Source Systems
The project integrates three source datasets:
1. Customers: Core customer information.
2. Transactions: Customer purchase and transaction history.
3. Contacts: Customer contact information from an
   independent source system.
## Engineering Capabilities
- File-level incremental ingestion.
- Reusable data quality framework.
- Invalid-record quarantine.
- Customer entity resolution.
- Slowly Changing Dimension Type 2.
- Golden customer master.
- Customer 360 analytics.
- RFM segmentation and CLV estimation.
- Cohort and retention analysis.
- Pipeline audit and reconciliation.

from pyspark.sql import functions as F #Import PySpark functions for data manipulation,column transformations, conditional logic and aggregations.
from pyspark.sql import types as T # Import data types to define explicit schemas and control the structure of our Delta tables.
from pyspark.sql.window import Window # Import Window functions for deduplication,ranking, running calculations and SCD processing.
from datetime import datetime, timezone # Import Python datetime utilities to generate processing timestamps and execution identifiers.
import uuid # Import UUID to generate a unique ID for every notebook run.
import re # Import regular expressions for reusable string validation and standardisation operations.
import json # Import JSON for serialising audit and configuration metadata.
import logging # Import logging to capture informational messages and errors.

PROJECT_NAME = "Customer Data Unification & Lakehouse Engineering" # Define the project name so that it can be reused in logs, audit records and documentation.
ENVIRONMENT = "Development" # Define the environment in which the notebook is running. This can later be changed to Development, Test or Production.
RUN_ID = str(uuid.uuid4()) # Generate a unique execution ID for this notebook run. This allows us to trace records processed in a particular run.
RUN_TIMESTAMP = datetime.now(timezone.utc) # Capture the current UTC timestamp for consistent processing metadata.

# Define the Lakehouse schema/database name. The notebook assumes that the attached Fabric Lakehouse
DATABASE_NAME = "customer360" # supports the schema-based table organisation we will use.
# Define the medallion layer names centrally.
BRONZE_SCHEMA = f"{DATABASE_NAME}.bronze"
SILVER_SCHEMA = f"{DATABASE_NAME}.silver"
GOLD_SCHEMA = f"{DATABASE_NAME}.gold"
OPS_SCHEMA = f"{DATABASE_NAME}.ops"
# Print the configuration to confirm initialisation.
print(f"Project       : {PROJECT_NAME}")
print(f"Environment   : {ENVIRONMENT}")
print(f"Run ID        : {RUN_ID}")
print(f"Run Timestamp : {RUN_TIMESTAMP}")
print(f"Database      : {DATABASE_NAME}")

# Define the landing folder where source CSV files will be uploaded to the attached Fabric Lakehouse.
LANDING_PATH = "Files/landing"
# Define the source datasets and their corresponding filenames.Keeping this configuration central avoids hardcoding filenames separately throughout the notebook.
SOURCE_FILES = {
    "customers": f"{LANDING_PATH}/customers.csv",
    "transactions": f"{LANDING_PATH}/transactions.csv",
    "contacts": f"{LANDING_PATH}/contacts.csv"
}

# Define the expected columns for each source dataset. These column names will be used to validate incoming files.
EXPECTED_COLUMNS = {
    "customers": [
        "customer_id",
        "full_name",
        "email",
        "phone",
        "city",
        "state",
        "date_of_birth",
        "created_at",
        "updated_at"
    ],
    "transactions": [
        "transaction_id",
        "customer_id",
        "transaction_date",
        "amount",
        "currency",
        "channel"
    ],
    "contacts": [
        "contact_id",
        "full_name",
        "email",
        "phone",
        "source_system"
    ]
}

# Display the configured source files and their expected columns.
for source_name, source_path in SOURCE_FILES.items():
    print(f"\nSource: {source_name}")
    print(f"File  : {source_path}")
    print(f"Columns: {EXPECTED_COLUMNS[source_name]}")

# Define Bronze table names.These tables will store the original ingested source records.
BRONZE_TABLES = {
    "customers": f"{BRONZE_SCHEMA}.customers",
    "transactions": f"{BRONZE_SCHEMA}.transactions",
    "contacts": f"{BRONZE_SCHEMA}.contacts"
}

# Define Silver table names. These tables will contain standardised and validated data.
SILVER_TABLES = {
    "customers": f"{SILVER_SCHEMA}.customers",
    "transactions": f"{SILVER_SCHEMA}.transactions",
    "contacts": f"{SILVER_SCHEMA}.contacts",
    "rejected_records": f"{SILVER_SCHEMA}.rejected_records",
    "entity_candidates": f"{SILVER_SCHEMA}.entity_match_candidates",
    "customer_history": f"{SILVER_SCHEMA}.customer_history"
}

# Define Gold table names.These tables will serve downstream analytical use cases.
GOLD_TABLES = {
    "customer_dimension": f"{GOLD_SCHEMA}.dim_customer",
    "transaction_fact": f"{GOLD_SCHEMA}.fact_transaction",
    "entity_master": f"{GOLD_SCHEMA}.golden_customer_master",
    "entity_xref": f"{GOLD_SCHEMA}.entity_cross_reference",
    "customer_360": f"{GOLD_SCHEMA}.customer_360",
    "monthly_spend": f"{GOLD_SCHEMA}.monthly_customer_spend",
    "rfm": f"{GOLD_SCHEMA}.customer_rfm",
    "clv": f"{GOLD_SCHEMA}.customer_clv",
    "cohort": f"{GOLD_SCHEMA}.customer_cohort_retention"
}

# Define operational table names. These tables will record ingestion progress and execution metrics.
OPS_TABLES = {
    "ingestion_manifest": f"{OPS_SCHEMA}.ingestion_manifest",
    "pipeline_audit": f"{OPS_SCHEMA}.pipeline_audit",
    "manual_review": f"{OPS_SCHEMA}.entity_manual_review"
}

# Print all configured table names for verification.
print("Bronze tables:", BRONZE_TABLES)
print("Silver tables:", SILVER_TABLES)
print("Gold tables:", GOLD_TABLES)
print("Operations tables:", OPS_TABLES)

CSV_FORMAT = "csv" # Define the CSV format used for all incoming source files.
CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "mode": "PERMISSIVE",
    "quote": '"',
    "escape": '"',
    "multiLine": "true"
}

DELTA_FORMAT = "delta" # Define the default Delta storage format for all medallion tables.
BUSINESS_TIMEZONE = "Asia/Kolkata" # Define the business timezone for interpreting source business dates. Processing timestamps will remain in UTC.
ENTITY_AUTO_MATCH_THRESHOLD = 0.95 # Define the default threshold for automatically approving high-confidence entity matches.
ENTITY_REVIEW_THRESHOLD = 0.70 # Define the minimum confidence score for sending a candidate to the manual review queue.
DATE_FORMAT = "yyyy-MM-dd" # Define the expected date format for the synthetic source datasets.
# Print the important processing settings.
print(f"CSV Format                   : {CSV_FORMAT}")
print(f"Delta Format                 : {DELTA_FORMAT}")
print(f"Business Timezone            : {BUSINESS_TIMEZONE}")
print(f"Auto Match Threshold         : {ENTITY_AUTO_MATCH_THRESHOLD}")
print(f"Manual Review Threshold      : {ENTITY_REVIEW_THRESHOLD}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}") # Create the Bronze schema to organise raw ingested tables.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}") # Create the Silver schema to organise cleaned and validated tables.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")# Create the Gold schema to organise curated analytical tables.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA}") # Create the Operations schema for ingestion manifests,pipeline audits and manual entity review records.
# Display a confirmation after schema creation.
print("Project schemas created or already available.")


# Display the schemas available in the current Spark catalog. This helps confirm whether the project schemas were created.
spark.sql("SHOW SCHEMAS").show(truncate=False)
# Display the active Spark session timezone. We explicitly configure the session to UTC so that timestamp processing is consistent across notebook runs.
spark.conf.set("spark.sql.session.timeZone", "UTC")
# Verify the configured session timezone.
print(
    "Spark session timezone:",
    spark.conf.get("spark.sql.session.timeZone")
)

# Print the current execution ID to help identify this run.
print("Current notebook run ID:", RUN_ID)
