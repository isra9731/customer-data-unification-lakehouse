# Fabric notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Customer Data Unification & Lakehouse Engineering
# MAGIC
# MAGIC **Platform:** Microsoft Fabric | **Processing:** PySpark | **Storage:** Delta Lake
# MAGIC
# MAGIC This notebook establishes the initial customer data pipeline:
# MAGIC source ingestion, Bronze storage, Silver standardisation and validation,
# MAGIC Gold customer and transaction outputs, and cross-system entity resolution.
# MAGIC
# MAGIC Attach the project Lakehouse as the default Lakehouse before running.
# MAGIC Upload the CSV files from `data/landing/` to `Files/landing/`.
# MAGIC
# MAGIC The pipeline is intentionally organised into clear stages. Incremental
# MAGIC processing, reusable quality rules, SCD Type 2, extended analytics and
# MAGIC operational monitoring will be added in subsequent implementation stages.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Configuration and setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LAKEHOUSE_SCHEMA = "customer360"
LANDING_PATH = "Files/landing"

# COMMAND ----------
# Create project schemas once. Keep the project tables grouped by medallion layer.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LAKEHOUSE_SCHEMA}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LAKEHOUSE_SCHEMA}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LAKEHOUSE_SCHEMA}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LAKEHOUSE_SCHEMA}.ops")

# COMMAND ----------
# Confirm the expected source files are available before ingestion.
required_files = {"customers.csv", "transactions.csv", "contacts.csv"}
available_files = {
    item.name.rstrip("/")
    for item in notebookutils.fs.ls(LANDING_PATH)
    if not item.isDir
}
missing_files = required_files - available_files
if missing_files:
    raise FileNotFoundError(
        f"Missing landing files: {sorted(missing_files)}. "
        f"Upload them to {LANDING_PATH} in the attached Lakehouse."
    )
display(notebookutils.fs.ls(LANDING_PATH))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Bronze — source ingestion
# MAGIC
# MAGIC Preserve source values as strings and add ingestion metadata. The initial
# MAGIC implementation uses a repeatable full refresh; watermark-based incremental
# MAGIC ingestion and Delta MERGE will be introduced in the next stage.

# COMMAND ----------
def ingest_bronze(file_name: str, target_table: str):
    source_path = f"{LANDING_PATH}/{file_name}"
    raw_df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load(source_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    raw_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target_table)
    return raw_df

bronze_customers = ingest_bronze(
    "customers.csv", f"{LAKEHOUSE_SCHEMA}.bronze.customers_raw"
)
bronze_transactions = ingest_bronze(
    "transactions.csv", f"{LAKEHOUSE_SCHEMA}.bronze.transactions_raw"
)
bronze_contacts = ingest_bronze(
    "contacts.csv", f"{LAKEHOUSE_SCHEMA}.bronze.contacts_raw"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Silver — standardisation and validation
# MAGIC
# MAGIC Parse source values safely, standardise common attributes, identify
# MAGIC duplicates and retain validation flags for downstream quality handling.

# COMMAND ----------
def safe_date(column):
    """Parse ISO and day/month/year dates; unparseable values become NULL."""
    return F.coalesce(
        F.to_date(column, "yyyy-MM-dd"),
        F.to_date(column, "dd/MM/yyyy"),
    )

# COMMAND ----------
# Customers: standardise fields and retain the latest row per customer key.
customers_typed = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.bronze.customers_raw")
    .select(
        F.upper(F.trim("customer_id")).alias("customer_id"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        safe_date(F.col("dob")).alias("dob"),
        F.lower(F.trim("email")).alias("email"),
        F.upper(F.trim("state")).alias("state"),
        F.to_timestamp("created_at").alias("created_at"),
        "_source_file",
        "_ingested_at",
    )
)

customer_window = Window.partitionBy("customer_id").orderBy(
    F.col("created_at").desc_nulls_last(),
    F.col("_ingested_at").desc(),
)
silver_customers = (
    customers_typed
    .withColumn("_row_number", F.row_number().over(customer_window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
    .withColumn(
        "is_valid_email",
        F.col("email").isNotNull()
        & F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"),
    )
    .withColumn("has_valid_dob", F.col("dob").isNotNull())
    .withColumn(
        "customer_quality_status",
        F.when(~F.col("is_valid_email"), "INVALID_EMAIL")
         .when(~F.col("has_valid_dob"), "INVALID_DOB")
         .otherwise("VALID"),
    )
)
silver_customers.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{LAKEHOUSE_SCHEMA}.silver.customers")

# COMMAND ----------
# Transactions: cast amounts, standardise fields, deduplicate transaction IDs,
# and check that each transaction references a known customer.
transactions_typed = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.bronze.transactions_raw")
    .select(
        F.upper(F.trim("txn_id")).alias("txn_id"),
        F.upper(F.trim("customer_id")).alias("customer_id"),
        safe_date(F.col("txn_date")).alias("txn_date"),
        F.expr("try_cast(amount AS DECIMAL(12,2))").alias("amount"),
        F.upper(F.trim("currency")).alias("currency"),
        F.lower(F.trim("channel")).alias("channel"),
        "_source_file",
        "_ingested_at",
    )
)
transaction_window = Window.partitionBy("txn_id").orderBy(
    F.col("_ingested_at").desc()
)
known_customers = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.silver.customers")
    .select("customer_id")
    .withColumn("_customer_exists", F.lit(True))
)
silver_transactions = (
    transactions_typed
    .withColumn("_row_number", F.row_number().over(transaction_window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
    .join(known_customers, "customer_id", "left")
    .withColumn(
        "has_known_customer",
        F.coalesce(F.col("_customer_exists"), F.lit(False)),
    )
    .drop("_customer_exists")
    .withColumn(
        "is_valid_amount",
        F.col("amount").isNotNull() & (F.col("amount") > 0),
    )
    .withColumn("has_valid_date", F.col("txn_date").isNotNull())
    .withColumn(
        "transaction_quality_status",
        F.when(~F.col("has_known_customer"), "UNKNOWN_CUSTOMER")
         .when(~F.col("is_valid_amount"), "INVALID_AMOUNT")
         .when(~F.col("has_valid_date"), "INVALID_DATE")
         .otherwise("VALID"),
    )
)
silver_transactions.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{LAKEHOUSE_SCHEMA}.silver.transactions")

# COMMAND ----------
# Contacts: standardise the second source for cross-system matching.
contact_window = Window.partitionBy("contact_id").orderBy(
    F.col("_ingested_at").desc()
)
silver_contacts = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.bronze.contacts_raw")
    .select(
        F.upper(F.trim("contact_id")).alias("contact_id"),
        F.trim("full_name").alias("full_name"),
        safe_date(F.col("date_of_birth")).alias("dob"),
        F.lower(F.trim("email")).alias("email"),
        F.initcap(F.trim("suburb")).alias("suburb"),
        F.upper(F.trim("state")).alias("state"),
        "_source_file",
        "_ingested_at",
    )
    .withColumn("_row_number", F.row_number().over(contact_window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
    .withColumn(
        "is_valid_email",
        F.col("email").isNotNull()
        & F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"),
    )
)
silver_contacts.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{LAKEHOUSE_SCHEMA}.silver.contacts")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver — unified person source and entity resolution
# MAGIC
# MAGIC Standardise names into comparable tokens, generate likely candidate pairs,
# MAGIC score candidates and classify outcomes. This first iteration uses explainable
# MAGIC deterministic rules; a review queue and richer scoring are planned.

# COMMAND ----------
def standardise_name(column):
    cleaned = F.upper(F.regexp_replace(column, r"[^A-Za-z ]", " "))
    tokens = F.filter(F.split(cleaned, r"\s+"), lambda token: F.length(token) > 0)
    return F.array_join(F.array_sort(tokens), " ")

crm_people = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.silver.customers")
    .select(
        F.lit("CRM").alias("source"),
        F.col("customer_id").alias("source_id"),
        F.concat_ws(" ", "first_name", "last_name").alias("raw_name"),
        "dob",
        F.when(F.col("is_valid_email"), F.col("email")).alias("email"),
        "state",
    )
)
case_people = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.silver.contacts")
    .select(
        F.lit("CASE").alias("source"),
        F.col("contact_id").alias("source_id"),
        F.col("full_name").alias("raw_name"),
        "dob",
        "email",
        "state",
    )
)
person_source = (
    crm_people.unionByName(case_people)
    .withColumn("name_std", standardise_name("raw_name"))
)
person_source.write.format("delta").mode("overwrite").saveAsTable(
    f"{LAKEHOUSE_SCHEMA}.silver.person_source"
)

# COMMAND ----------
crm = (
    person_source.where(F.col("source") == "CRM")
    .select(
        F.col("source_id").alias("crm_id"),
        F.col("name_std").alias("crm_name"),
        F.col("dob").alias("crm_dob"),
        F.col("email").alias("crm_email"),
    )
)
case = (
    person_source.where(F.col("source") == "CASE")
    .select(
        F.col("source_id").alias("case_id"),
        F.col("name_std").alias("case_name"),
        F.col("dob").alias("case_dob"),
        F.col("email").alias("case_email"),
    )
)

def soundex_tokens(column):
    return F.transform(F.split(column, " "), lambda token: F.soundex(token))

candidate_condition = (
    (crm["crm_email"].isNotNull() & (crm["crm_email"] == case["case_email"]))
    | (crm["crm_dob"].isNotNull() & (crm["crm_dob"] == case["case_dob"]))
    | F.arrays_overlap(
        soundex_tokens(F.col("crm_name")),
        soundex_tokens(F.col("case_name")),
    )
)
name_similarity = F.round(
    1 - (
        F.levenshtein(F.col("crm_name"), F.col("case_name"))
        / F.greatest(F.length("crm_name"), F.length("case_name"))
    ),
    3,
)

scored_pairs = (
    crm.join(case, candidate_condition, "inner")
    .withColumn("name_similarity", name_similarity)
    .withColumn(
        "match_status",
        F.when(
            F.col("crm_email").isNotNull()
            & (F.col("crm_email") == F.col("case_email")),
            "MATCH",
        )
        .when(
            (F.col("crm_dob") == F.col("case_dob"))
            & (F.col("name_similarity") >= 0.70),
            "MATCH",
        )
        .when(F.col("name_similarity") >= 0.85, "REVIEW")
        .otherwise("NO_MATCH"),
    )
    .withColumn(
        "match_reason",
        F.when(
            F.col("crm_email").isNotNull()
            & (F.col("crm_email") == F.col("case_email")),
            "EXACT_EMAIL",
        )
        .when(
            (F.col("crm_dob") == F.col("case_dob"))
            & (F.col("name_similarity") >= 0.70),
            "DOB_AND_NAME",
        )
        .when(F.col("name_similarity") >= 0.85, "NAME_SIMILARITY_ONLY")
        .otherwise("INSUFFICIENT_EVIDENCE"),
    )
)
scored_pairs.write.format("delta").mode("overwrite").saveAsTable(
    f"{LAKEHOUSE_SCHEMA}.silver.entity_match_candidates"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Gold — customer, transaction and golden-entity outputs

# COMMAND ----------
age_years = F.floor(F.datediff(F.current_date(), F.col("dob")) / 365.25)
gold_customer = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.silver.customers")
    .select(
        "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
        "dob",
        F.when(F.col("dob").isNull(), "Unknown")
         .when(age_years < 30, "Under 30")
         .when(age_years < 50, "30-49")
         .otherwise("50+").alias("age_band"),
        "state",
        F.when(F.col("is_valid_email"), F.col("email")).alias("email"),
    )
)
gold_customer.write.format("delta").mode("overwrite").saveAsTable(
    f"{LAKEHOUSE_SCHEMA}.gold.dim_customer"
)

gold_monthly_spend = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.silver.transactions")
    .where(
        F.col("has_known_customer")
        & F.col("is_valid_amount")
        & F.col("has_valid_date")
    )
    .groupBy(
        "customer_id",
        F.date_trunc("month", "txn_date").alias("txn_month"),
        "channel",
        "currency",
    )
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_spend"),
        F.avg("amount").alias("average_transaction_value"),
    )
)
gold_monthly_spend.write.format("delta").mode("overwrite").saveAsTable(
    f"{LAKEHOUSE_SCHEMA}.gold.fct_monthly_spend"
)

# Resolve only high-confidence matches in this initial pass.
best_match_window = Window.partitionBy("case_id").orderBy(
    F.col("name_similarity").desc()
)
resolved_matches = (
    spark.table(f"{LAKEHOUSE_SCHEMA}.silver.entity_match_candidates")
    .where(F.col("match_status") == "MATCH")
    .withColumn("_rank", F.row_number().over(best_match_window))
    .where(F.col("_rank") == 1)
    .select("case_id", "crm_id")
)

crm_xref = (
    person_source.where(F.col("source") == "CRM")
    .select(
        F.concat(F.lit("ENT-"), "source_id").alias("entity_id"),
        "source",
        "source_id",
    )
)
case_xref = (
    person_source.where(F.col("source") == "CASE")
    .join(resolved_matches, F.col("source_id") == F.col("case_id"), "left")
    .select(
        F.coalesce(
            F.concat(F.lit("ENT-"), F.col("crm_id")),
            F.concat(F.lit("ENT-"), F.col("source_id")),
        ).alias("entity_id"),
        "source",
        "source_id",
    )
)
entity_xref = crm_xref.unionByName(case_xref)
entity_xref.write.format("delta").mode("overwrite").saveAsTable(
    f"{LAKEHOUSE_SCHEMA}.gold.entity_xref"
)

entity_master = (
    entity_xref.join(person_source, ["source", "source_id"])
    .groupBy("entity_id")
    .agg(
        F.max(F.when(F.col("source") == "CRM", F.initcap(F.lower("raw_name"))))
         .alias("crm_name"),
        F.max(F.when(F.col("source") == "CASE", F.initcap(F.lower("raw_name"))))
         .alias("case_name"),
        F.max("dob").alias("dob"),
        F.array_distinct(F.collect_list("email")).alias("emails"),
        F.array_distinct(F.collect_list("state")).alias("states"),
        F.collect_list(F.struct("source", "source_id")).alias("source_records"),
        F.count("*").alias("record_count"),
    )
    .withColumn("display_name", F.coalesce("crm_name", "case_name"))
)
entity_master.write.format("delta").mode("overwrite").saveAsTable(
    f"{LAKEHOUSE_SCHEMA}.gold.entity_master"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Initial validation and inspection
# MAGIC
# MAGIC These checks provide a quick baseline. A reusable assertion-based test suite,
# MAGIC data quality metrics and pipeline run logging will be added in later stages.

# COMMAND ----------
tables_to_check = [
    f"{LAKEHOUSE_SCHEMA}.bronze.customers_raw",
    f"{LAKEHOUSE_SCHEMA}.bronze.transactions_raw",
    f"{LAKEHOUSE_SCHEMA}.bronze.contacts_raw",
    f"{LAKEHOUSE_SCHEMA}.silver.customers",
    f"{LAKEHOUSE_SCHEMA}.silver.transactions",
    f"{LAKEHOUSE_SCHEMA}.silver.contacts",
    f"{LAKEHOUSE_SCHEMA}.silver.entity_match_candidates",
    f"{LAKEHOUSE_SCHEMA}.gold.dim_customer",
    f"{LAKEHOUSE_SCHEMA}.gold.fct_monthly_spend",
    f"{LAKEHOUSE_SCHEMA}.gold.entity_xref",
    f"{LAKEHOUSE_SCHEMA}.gold.entity_master",
]
for table_name in tables_to_check:
    print(f"{table_name}: {spark.table(table_name).count()} rows")

display(spark.table(f"{LAKEHOUSE_SCHEMA}.gold.dim_customer"))
display(spark.table(f"{LAKEHOUSE_SCHEMA}.gold.fct_monthly_spend"))
display(
    spark.table(f"{LAKEHOUSE_SCHEMA}.gold.entity_master")
    .select("entity_id", "display_name", "record_count", "source_records")
    .orderBy("entity_id")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Implementation roadmap
# MAGIC
# MAGIC - Incremental ingestion with source watermarks and Delta MERGE
# MAGIC - Reusable data quality rules and rejected-record quarantine
# MAGIC - SCD Type 2 customer history
# MAGIC - Enhanced entity matching confidence, explanations and review workflow
# MAGIC - Customer 360 analytics: RFM, cohort retention and lifetime value
# MAGIC - Pipeline audit, execution metrics and automated tests
