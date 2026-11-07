# 01 - Bronze Layer: Raw Data Ingestion
## Objective
The Bronze layer is responsible for ingesting raw source data into the Microsoft Fabric Lakehouse. The original source values are preserved without applying
business transformations or data cleansing.

## Source datasets
1. Customers
2. Transactions
3. Contacts

## Key processing activities
- Validate source file availability.
- Validate incoming CSV headers.
- Read source files with schema inference disabled.
- Add ingestion metadata.
- Persist raw records as Delta tables.
- Maintain a file-level ingestion manifest.
- Support reprocessing when a source file changes.
- Validate ingested record counts.

## Outputs
- bronze.customers
- bronze.transactions
- bronze.contacts
- ops.ingestion_manifest

# Import the Fabric notebook utilities used to inspect
# files stored in the attached Lakehouse.
from notebookutils import mssparkutils

# List all files available in the configured landing folder.
# This helps us identify which source files are available
# before starting the ingestion process.
landing_files = mssparkutils.fs.ls(LANDING_PATH)

# Extract the available filenames into a Python set.
# A set makes it efficient to check whether a required
# filename exists in the landing folder.
available_files = {
    file_info.name for file_info in landing_files
}

# Create a list of required source filenames.
required_files = {
    "customers.csv",
    "transactions.csv",
    "contacts.csv"
}
# Identify source files that are missing from the landing folder.
missing_files = required_files - available_files
# Stop the notebook if any required source files are missing. This prevents partial ingestion caused by unavailable inputs.
if missing_files:
    raise FileNotFoundError(
        f"Missing required source files: {missing_files}"
    )
print("All required source files are available.") 
# Display the source files detected in the landing folder.
for file_info in landing_files:
    if file_info.name in required_files:
        print(
            f"File: {file_info.name}, "
            f"Size: {file_info.size} bytes"
        )

# Definining a reusable function for reading and validating raw CSV source files.
def read_source_csv(source_name, source_path):
    expected_columns = EXPECTED_COLUMNS[source_name] # Retrieve the expected column names for this source from the configuration notebook.    
    source_df = (     # Read the CSV file using the centrally defined CSV options Schema inference is disabled so that Bronze preserves
        spark.read
        .format(CSV_FORMAT)
        .options(**CSV_OPTIONS)
        .load(source_path)
    )
    actual_columns = source_df.columns     # Retrieve the actual column names from the incoming CSV.
    missing_columns = set(expected_columns) - set(actual_columns)  # Identify columns expected by the project but missing from the incoming source file.
    unexpected_columns = set(actual_columns) - set(expected_columns) # Identify unexpected columns that are not in our currently configured source schema.
    if missing_columns:     # Stop processing when mandatory expected columns are missing. We do not silently create or substitute missing source fields.
        raise ValueError(
            f"{source_name}: Missing columns: {missing_columns}"
        )
    if unexpected_columns:     # Report unexpected columns for visibility.These are retained in Bronze rather than discarded.
        print(
            f"Warning: {source_name} contains additional columns: "
            f"{unexpected_columns}"
        )
    return source_df     # Return the original DataFrame without business transformations.

# Define a reusable function that adds ingestion metadata to a raw source DataFrame.
def add_bronze_metadata(
    source_df,
    source_path,
    source_size,
    source_modified_at
):
    # Add the original source file path. This helps trace each record back to its source.
    source_df = source_df.withColumn(
        "_source_file",
        F.lit(source_path)
    )
    # Record the source file size. This is part of our file-level change tracking.
    source_df = source_df.withColumn(
        "_source_file_size",
        F.lit(source_size).cast("long")
    )
    # Record the source file modification timestamp.This helps identify when a previously processed file changes.
    source_df = source_df.withColumn(
        "_source_file_modified_at",
        F.lit(str(source_modified_at))
    )
    # Add the UTC ingestion timestamp.This records when the data was loaded into Bronze.
    source_df = source_df.withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
    # Attach the unique notebook execution ID.This allows us to trace all records ingested in this run.
    source_df = source_df.withColumn(
        "_run_id",
        F.lit(RUN_ID)
    )
    return source_df  # Return the DataFrame containing original source fields and the additional Bronze metadata.

# Define an explicit schema for the ingestion manifest. Using an explicit schema keeps the operational table consistent across notebook executions.
manifest_schema = T.StructType([
    T.StructField("source_name", T.StringType(), False),     # Identify the source dataset, such as customers or transactions.
    T.StructField("source_file", T.StringType(), False),     # Store the complete source file path.
    T.StructField("source_file_size", T.LongType(), True),    # Store the source file size used for change detection.
    T.StructField("source_file_modified_at", T.StringType(), True),     # Store the source file modification timestamp.
    T.StructField("records_ingested", T.LongType(), True),    # Store the number of records ingested from this file.
    T.StructField("run_id", T.StringType(), False),    # Store the unique notebook execution ID.
    T.StructField("processed_at", T.TimestampType(), True),    # Store the time when the file was processed.
    # Store the processing status.
    T.StructField("status", T.StringType(), False)
])

# Retrieve the configured ingestion manifest table name.
manifest_table = OPS_TABLES["ingestion_manifest"]
# Check whether the manifest table already exists.If it exists, we will use its existing records to determine which files have already been processed.
manifest_exists = spark.catalog.tableExists(manifest_table)
# Create the manifest table if it does not already exist. An empty DataFrame with the predefined schema is used to initialise the Delta table.
if not manifest_exists:
    empty_manifest_df = spark.createDataFrame(     # Create an empty DataFrame with the manifest schema.
        [],
        schema=manifest_schema
    )
    # Save the empty DataFrame as a Delta table.
    empty_manifest_df.write \
        .format(DELTA_FORMAT) \
        .mode("overwrite") \
        .saveAsTable(manifest_table)
    print("Ingestion manifest table created.")
else:
    # Confirm that the existing manifest is available.
    print("Existing ingestion manifest found.")
# Load the manifest so it can be used during incremental processing.
manifest_df = spark.table(manifest_table)
# Display existing ingestion history.
display(manifest_df)

# Define a reusable function for file-level incremental ingestion.
def ingest_source_file(source_name, source_path):
    files_in_landing = mssparkutils.fs.ls(LANDING_PATH)     # List the landing directory to retrieve the latest source file metadata.
    file_name = source_path.split("/")[-1]    # Identify the requested source file.
    matching_files = [
        file_info
        for file_info in files_in_landing
        if file_info.name == file_name
    ]
    # Stop processing if the requested file cannot be found.
    if not matching_files:
        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )
    file_info = matching_files[0]     # Extract the source file metadata.
    source_size = int(file_info.size)
    source_modified_at = str(
        getattr(file_info, "modifyTime", None)
    )
    target_table = BRONZE_TABLES[source_name]     # Identify the Bronze destination table.
    target_exists = spark.catalog.tableExists(target_table)    # Check whether the Bronze table already exists.
    previous_entries = (     # Read the latest manifest entry for this source file. The latest processing timestamp is used if multiple manifest entries exist for the same source.
        spark.table(manifest_table)
        .filter(F.col("source_file") == source_path)
        .orderBy(F.col("processed_at").desc())
        .limit(1)
        .collect()
    )
    if previous_entries:     # Determine whether the source file has already been processed with the same file size and modification time.
        previous_entry = previous_entries[0]
        file_unchanged = (
            previous_entry["source_file_size"] == source_size
            and previous_entry["source_file_modified_at"]
                == source_modified_at
            and previous_entry["status"] == "SUCCESS"
        )
    else:
        file_unchanged = False         # A missing manifest entry means this is a new file.
    if file_unchanged:    # Skip the file when its metadata has not changed.
        print(
            f"Skipping {source_name}: "
            "file has already been processed and is unchanged."
        )
        return {
            "source_name": source_name,
            "status": "SKIPPED",
            "records_ingested": 0
        }
    # Read the raw source CSV and validate its headers.
    source_df = read_source_csv(
        source_name,
        source_path
    )
    # Add Bronze ingestion metadata to every source record.
    bronze_df = add_bronze_metadata(
        source_df,
        source_path,
        source_size,
        source_modified_at
    )
    records_ingested = bronze_df.count()     # Count records before writing them to Bronze. This count will be stored in the ingestion manifest.
    # If the Bronze table exists, remove the previous records associated with this source file. This supports safe replacement when the file changes or when a previous run needs to be retried.
    if target_exists:
        safe_source_path = source_path.replace("'", "''")         # Escape single quotes in the file path before using it in the SQL DELETE statement.
        # Delete records from the previous version of this file.
        spark.sql(f"""
            DELETE FROM {target_table}
            WHERE _source_file = '{safe_source_path}'
        """)
        bronze_df.write \         # Append the latest source file records to the Bronze table.
            .format(DELTA_FORMAT) \
            .mode("append") \
            .saveAsTable(target_table)
    else:
        bronze_df.write \         # Create the Bronze Delta table for the first time.
            .format(DELTA_FORMAT) \
            .mode("overwrite") \
            .saveAsTable(target_table)
    # Create a manifest record describing this ingestion.
    manifest_record = [(
        source_name,
        source_path,
        source_size,
        source_modified_at,
        records_ingested,
        RUN_ID,
        datetime.now(timezone.utc),
        "SUCCESS"
    )]
    # Convert the manifest record into a DataFrame using the explicit manifest schema.
    manifest_record_df = spark.createDataFrame(
        manifest_record,
        schema=manifest_schema
    )
    # Append the successful ingestion record to the manifest.
    manifest_record_df.write \
        .format(DELTA_FORMAT) \
        .mode("append") \
        .saveAsTable(manifest_table)
    print(
        f"Ingested {source_name}: "
        f"{records_ingested} records."
    )
    # Return the ingestion summary.
    return {
        "source_name": source_name,
        "status": "SUCCESS",
        "records_ingested": records_ingested
    }
# Create a list to capture the ingestion results returned by each source processing operation.
ingestion_results = []
for source_name, source_path in SOURCE_FILES.items():
    result = ingest_source_file(
        source_name,
        source_path
    )
    ingestion_results.append(result)    # Append the result to the execution summary.
ingestion_summary_df = spark.createDataFrame(
    ingestion_results
)
display(ingestion_summary_df) # Display the status and record counts for each source.
bronze_counts = {} # Create a dictionary to store the total row count of each Bronze table after ingestion.
for source_name, table_name in BRONZE_TABLES.items():
    bronze_df = spark.table(table_name)
    record_count = bronze_df.count()    # Count the records currently available in the table.
    bronze_counts[source_name] = record_count     # Store the count for the final validation summary.
    # Display the result for the current source.
    print(
        f"Bronze table: {table_name} | "
        f"Record count: {record_count}"
    )
# Convert the counts into a DataFrame for display.
bronze_validation_df = spark.createDataFrame(
    [
        (source_name, count)
        for source_name, count in bronze_counts.items()
    ],
    ["source_name", "bronze_record_count"]
)
# Display the complete Bronze validation summary.
display(bronze_validation_df)
display(
  spark.table(BRONZE_TABLES["customers"]) # Display the raw customer records to verify that the original source values and metadata are available.
       )
display(
  spark.table(BRONZE_TABLES["transactions"]) # Display raw transaction records and their ingestion metadata.
)
display(
    spark.table(BRONZE_TABLES["contacts"])# Display raw contact records and their ingestion metadata.
)
# Load the latest ingestion manifest records.The manifest provides a history of files processed by this notebook across different executions.
latest_manifest_df = (
    spark.table(manifest_table)
    .orderBy(F.col("processed_at").desc())
)
# Display the manifest so that we can verify the ingestion status, source file and record counts.
display(latest_manifest_df)

  
