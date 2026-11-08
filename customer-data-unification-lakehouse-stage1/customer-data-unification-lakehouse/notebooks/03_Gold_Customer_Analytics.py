# Import PySpark functions for transformations and aggregations.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Set the project database and table names.
DATABASE_NAME = "customer360"

SILVER_CUSTOMERS = f"{DATABASE_NAME}.silver.customers"
SILVER_TRANSACTIONS = f"{DATABASE_NAME}.silver.transactions"

GOLD_CUSTOMERS = f"{DATABASE_NAME}.gold.dim_customer"
GOLD_TRANSACTIONS = f"{DATABASE_NAME}.gold.fact_transaction"
GOLD_MONTHLY_SPEND = f"{DATABASE_NAME}.gold.monthly_customer_spend"
GOLD_CUSTOMER_360 = f"{DATABASE_NAME}.gold.customer_360"
GOLD_RFM = f"{DATABASE_NAME}.gold.customer_rfm"

# Create the Gold schema if it does not already exist.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.gold")
# Read trusted customer records from the Silver layer.
silver_customers_df = spark.table(SILVER_CUSTOMERS)
# Read trusted transaction records from the Silver layer.
silver_transactions_df = spark.table(SILVER_TRANSACTIONS)
# Display the source records for verification.
print("Silver customers:", silver_customers_df.count())
print("Silver transactions:", silver_transactions_df.count())

# Select the customer attributes required for the Gold dimension.
dim_customer_df = silver_customers_df.select(
    "customer_id",
    "full_name",
    "email",
    "phone",
    "city",
    "state",
    "date_of_birth",
    "created_at",
    "updated_at"
)
# Add a timestamp indicating when the Gold record was created.
dim_customer_df = dim_customer_df.withColumn(
    "gold_created_at",
    F.current_timestamp()
)
# Save the customer dimension as a Delta table.
dim_customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_CUSTOMERS)

# Select the transaction attributes required for analytics.
fact_transaction_df = silver_transactions_df.select(
    "transaction_id",
    "customer_id",
    "transaction_date",
    "amount",
    "currency",
    "channel"
)

# Add a Gold processing timestamp.
fact_transaction_df = fact_transaction_df.withColumn(
    "gold_created_at",
    F.current_timestamp()
)

# Save the transaction fact table.
fact_transaction_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TRANSACTIONS)

# Aggregate customer transactions by month and currency.
monthly_spend_df = fact_transaction_df.groupBy(
    "customer_id",
    F.date_format("transaction_date", "yyyy-MM").alias("transaction_month"),
    "currency"
).agg(
    F.sum("amount").alias("total_spend"),
    F.countDistinct("transaction_id").alias("transaction_count"),
    F.avg("amount").alias("average_transaction_amount")
)

# Save monthly spending analytics.
monthly_spend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_MONTHLY_SPEND)

# Calculate customer-level transaction metrics.
customer_metrics_df = fact_transaction_df.groupBy(
    "customer_id"
).agg(
    F.countDistinct("transaction_id").alias("total_transactions"),
    F.sum("amount").alias("total_spend"),
    F.avg("amount").alias("average_transaction_value"),
    F.max("transaction_date").alias("last_transaction_date")
)

# Join customer information with transaction metrics.
customer_360_df = dim_customer_df.join(
    customer_metrics_df,
    on="customer_id",
    how="left"
)

# Replace missing transaction counts and spending values with zero.
customer_360_df = customer_360_df.fillna({
    "total_transactions": 0,
    "total_spend": 0
})

# Save the unified Customer 360 dataset.
customer_360_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_CUSTOMER_360)

# Use the latest transaction date as the reference date for recency.
reference_date = fact_transaction_df.select(
    F.max("transaction_date").alias("max_date")
).first()["max_date"]

# Calculate customer-level RFM metrics.
rfm_df = fact_transaction_df.groupBy(
    "customer_id"
).agg(
    F.datediff(
        F.lit(reference_date),
        F.max("transaction_date")
    ).alias("recency_days"),

    F.countDistinct("transaction_id").alias("frequency"),

    F.sum("amount").alias("monetary_value")
)

# Assign scores from 1 to 5 using customer rankings.
recency_window = Window.orderBy(F.col("recency_days").asc())
frequency_window = Window.orderBy(F.col("frequency").asc())
monetary_window = Window.orderBy(F.col("monetary_value").asc())

rfm_df = rfm_df \
    .withColumn("recency_score", F.ntile(5).over(recency_window)) \
    .withColumn("frequency_score", F.ntile(5).over(frequency_window)) \
    .withColumn("monetary_score", F.ntile(5).over(monetary_window))

# Calculate a combined RFM score.
rfm_df = rfm_df.withColumn(
    "rfm_score",
    F.col("recency_score")
    + F.col("frequency_score")
    + F.col("monetary_score")
)

# Assign simple customer segments based on the combined score.
rfm_df = rfm_df.withColumn(
    "customer_segment",
    F.when(F.col("rfm_score") >= 12, "High Value")
     .when(F.col("rfm_score") >= 9, "Loyal")
     .when(F.col("rfm_score") >= 6, "Potential")
     .otherwise("At Risk")
)

# Save customer segmentation results.
rfm_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_RFM)

# Check that all Gold tables were created successfully.
gold_tables = [
    GOLD_CUSTOMERS,
    GOLD_TRANSACTIONS,
    GOLD_MONTHLY_SPEND,
    GOLD_CUSTOMER_360,
    GOLD_RFM
]

# Display the record count for each Gold table.
for table_name in gold_tables:
    print(f"{table_name}: {spark.table(table_name).count()} records")
