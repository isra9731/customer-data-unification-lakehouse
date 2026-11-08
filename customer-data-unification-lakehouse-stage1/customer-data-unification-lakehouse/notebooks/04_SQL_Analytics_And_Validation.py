# Import PySpark functions for validation and reporting.
from pyspark.sql import functions as F

# Define the project database.
DATABASE_NAME = "customer360"

# Define the Gold table names.
DIM_CUSTOMER = f"{DATABASE_NAME}.gold.dim_customer"
FACT_TRANSACTION = f"{DATABASE_NAME}.gold.fact_transaction"
CUSTOMER_360 = f"{DATABASE_NAME}.gold.customer_360"
MONTHLY_SPEND = f"{DATABASE_NAME}.gold.monthly_customer_spend"
CUSTOMER_RFM = f"{DATABASE_NAME}.gold.customer_rfm"

# Define the Silver table names.
SILVER_CUSTOMERS = f"{DATABASE_NAME}.silver.customers"
SILVER_TRANSACTIONS = f"{DATABASE_NAME}.silver.transactions"
REJECTED_RECORDS = f"{DATABASE_NAME}.silver.rejected_records"

# Register Gold tables as temporary SQL views.
spark.table(DIM_CUSTOMER).createOrReplaceTempView("dim_customer")
spark.table(FACT_TRANSACTION).createOrReplaceTempView("fact_transaction")
spark.table(CUSTOMER_360).createOrReplaceTempView("customer_360")
spark.table(MONTHLY_SPEND).createOrReplaceTempView("monthly_customer_spend")
spark.table(CUSTOMER_RFM).createOrReplaceTempView("customer_rfm")

#Top 10 customers by total spending
SELECT
    customer_id,
    full_name,
    total_transactions,
    total_spend
FROM customer_360
ORDER BY total_spend DESC
LIMIT 10;

#Monthly revenue
SELECT
    transaction_month,
    currency,
    SUM(total_spend) AS monthly_revenue,
    SUM(transaction_count) AS total_transactions
FROM monthly_customer_spend
GROUP BY transaction_month, currency
ORDER BY transaction_month;

#Customer segmentation
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(monetary_value), 2) AS average_customer_spend
FROM customer_rfm
GROUP BY customer_segment
ORDER BY customer_count DESC;

#Customers who have not transacted recently
SELECT
    customer_id,
    full_name,
    last_transaction_date,
    total_spend
FROM customer_360
WHERE last_transaction_date < DATE_SUB(
    CURRENT_DATE(), 90
)
ORDER BY last_transaction_date;

#Transaction channel analysis
SELECT
    channel,
    currency,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount
FROM fact_transaction
GROUP BY channel, currency
ORDER BY transaction_count DESC;

#Execute SQL queries
# Execute the customer spending query.
top_customers_df = spark.sql("""
    SELECT
        customer_id,
        full_name,
        total_transactions,
        total_spend
    FROM customer_360
    ORDER BY total_spend DESC
    LIMIT 10
""")

# Display the top customers.
display(top_customers_df)

# Execute and display monthly revenue analytics.
display(spark.sql("""
    SELECT
        transaction_month,
        currency,
        SUM(total_spend) AS monthly_revenue,
        SUM(transaction_count) AS total_transactions
    FROM monthly_customer_spend
    GROUP BY transaction_month, currency
    ORDER BY transaction_month
"""))

# Execute and display customer segmentation analytics.
display(spark.sql("""
    SELECT
        customer_segment,
        COUNT(*) AS customer_count,
        ROUND(AVG(monetary_value), 2) AS average_customer_spend
    FROM customer_rfm
    GROUP BY customer_segment
    ORDER BY customer_count DESC
"""))

#Validate primary keys
# Check for duplicate customer IDs in the Gold customer dimension.
duplicate_customers = spark.table(DIM_CUSTOMER) \
    .groupBy("customer_id") \
    .count() \
    .filter(F.col("count") > 1)

# Check for duplicate transaction IDs in the Gold fact table.
duplicate_transactions = spark.table(FACT_TRANSACTION) \
    .groupBy("transaction_id") \
    .count() \
    .filter(F.col("count") > 1)

# Display duplicate records, if any.
print("Duplicate customer IDs:", duplicate_customers.count())
print("Duplicate transaction IDs:", duplicate_transactions.count())

#Validate referential integrity
# Find transactions whose customer IDs do not exist in the customer dimension.
invalid_customer_references = (
    spark.table(FACT_TRANSACTION)
    .join(
        spark.table(DIM_CUSTOMER).select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
)

# Display the number of transactions with invalid customer references.
print(
    "Invalid customer references:",
    invalid_customer_references.count()
)

#Validate Gold data completeness
# Calculate null customer IDs in the Gold dimension.
null_customer_ids = spark.table(DIM_CUSTOMER) \
    .filter(F.col("customer_id").isNull()) \
    .count()

# Calculate null transaction IDs in the Gold fact table.
null_transaction_ids = spark.table(FACT_TRANSACTION) \
    .filter(F.col("transaction_id").isNull()) \
    .count()

# Calculate invalid transaction amounts.
invalid_amounts = spark.table(FACT_TRANSACTION) \
    .filter(F.col("amount").isNull()) \
    .count()

# Display validation results.
print("Null customer IDs:", null_customer_ids)
print("Null transaction IDs:", null_transaction_ids)
print("Null transaction amounts:", invalid_amounts)

#Final layer record counts
# Define the tables to include in the final project summary.
project_tables = [
    f"{DATABASE_NAME}.bronze.customers",
    f"{DATABASE_NAME}.bronze.transactions",
    f"{DATABASE_NAME}.bronze.contacts",
    f"{DATABASE_NAME}.silver.customers",
    f"{DATABASE_NAME}.silver.transactions",
    f"{DATABASE_NAME}.silver.contacts",
    DIM_CUSTOMER,
    FACT_TRANSACTION,
    CUSTOMER_360,
    MONTHLY_SPEND,
    CUSTOMER_RFM,
    REJECTED_RECORDS
]

# Collect the record count for each table.
final_counts = []

for table_name in project_tables:

    # Count records in the current table.
    record_count = spark.table(table_name).count()

    # Store the table name and its record count.
    final_counts.append((table_name, record_count))

# Create a DataFrame containing the project record counts.
final_summary_df = spark.createDataFrame(
    final_counts,
    ["table_name", "record_count"]
)

# Display the final project summary.
display(final_summary_df)

#Final validation summary
# Collect the validation metrics into a single summary.
validation_results = [
    ("Duplicate customer IDs", duplicate_customers.count()),
    ("Duplicate transaction IDs", duplicate_transactions.count()),
    ("Invalid customer references", invalid_customer_references.count()),
    ("Null customer IDs", null_customer_ids),
    ("Null transaction IDs", null_transaction_ids),
    ("Null transaction amounts", invalid_amounts)
]

# Create the validation summary DataFrame.
validation_summary_df = spark.createDataFrame(
    validation_results,
    ["validation_check", "failed_records"]
)

# Mark checks as PASS when no failed records are found.
validation_summary_df = validation_summary_df.withColumn(
    "validation_status",
    F.when(F.col("failed_records") == 0, "PASS")
     .otherwise("FAIL")
)

# Display the final validation report.
display(validation_summary_df)

