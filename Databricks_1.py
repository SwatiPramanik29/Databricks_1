# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.source_table")  # Create source schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.target_table")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.source_table")  # Create source schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.target_table")

# COMMAND ----------

sales = spark.sql("SELECT * FROM samples.accuweather.forecast_daily_calendar_imperial")
sales.write.mode("overwrite").saveAsTable("workspace.source_table.sales")

# COMMAND ----------

#Load Data From Source
source = spark.read.table('workspace.source_table.sales')
source.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Load Data From Source and concatenate all columns into 'ConCatValue'
source = source.withColumn('ConCatValue', F.concat_ws('', *source.columns))
display(source)


# COMMAND ----------

# Add IndCurrent, CreatedDate, and ModifiedDate columns
source = source.withColumn("IndCurrent", F.lit(1)) \
    .withColumn("CreatedDate", F.current_timestamp()) \
    .withColumn("ModifiedDate", F.current_timestamp())
source.display()

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.orderBy(F.monotonically_increasing_id())
source = source.withColumn("storage_id", F.row_number().over(window_spec))

first_cols = ["storage_id"]
other_cols = [col for col in source.columns if col not in first_cols]
source = source.select(first_cols + other_cols)

display(source)

# COMMAND ----------

# Generate SHA-256 hash of concatenated column values and drop 'ConCatValue'
source = source.withColumn("RowHash", F.sha2(F.col("ConCatValue"), 256)).drop('ConCatValue')
display(source)


# COMMAND ----------

#writing to the target schema  
source.write.mode("append").saveAsTable("workspace.target_table.sales")


# COMMAND ----------

# Display data from the target_table schema
target_df = spark.sql("SELECT * FROM workspace.target_table.sales")
display(target_df)

# COMMAND ----------

SourceTable='workspace.source_table.sales'
TargetTable='workspace.target_table.sales'

# COMMAND ----------

SourceDf=spark.read.table(SourceTable)  # Read source table into DataFrame
TargetDf=spark.read.table(TargetTable)  # Read target table into DataFrame


# COMMAND ----------

SourceDf.display()

# COMMAND ----------

from pyspark.sql.functions import col

# Filter the DataFrame to show only rows where 'degree_days_freezing' is '0'
# Display the filtered DataFrame for inspection
SourceDf.filter(col("degree_days_freezing") == "0").display()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Update the 'city_name' column in SourceDf:
# For rows where 'degree_days_freezing' equals '0', set the 'city_name' value to 'Kolkata'.
# For all other rows, retain the original 'city_name' value.
SourceDf = SourceDf.withColumn(
    "city_name",
    when(col("degree_days_freezing") == "0", "Kolkata").otherwise(col("city_name"))
)

# Display rows where 'degree_days_freezing' is '0' to verify the 'city_name' column update.
SourceDf.filter(col("degree_days_freezing") == "0").display()

# After this update, the 'city_name' value for all rows with 'degree_days_freezing' 0 will be 'Kolkata'.
     

# COMMAND ----------

# Create a hash key by concatenating all columns into a single string column 'RowHash'
from pyspark.sql import functions as F

# Concatenate all columns in 'source' DataFrame into 'RowHash'
SourceDf = SourceDf.withColumn('RowHash', F.concat_ws('', *SourceDf.columns))

# COMMAND ----------

# Add three new columns to SourceDf:
# 1. 'IndCurrent': Set to 1 for all rows, indicating the current/active record.
# 2. 'CreatedDate': Set to the current timestamp, representing when the record was created.
# 3. 'ModifiedDate': Set to the current timestamp, representing when the record was last modified.
SourceDf = SourceDf.withColumn("IndCurrent", F.lit(1)) \
    .withColumn("CreatedDate", F.current_timestamp()) \
    .withColumn("ModifiedDate", F.current_timestamp())

# COMMAND ----------

# Add three new columns to SourceDf:
# 1. 'IndCurrent': Set to 1 for all rows, indicating the current/active record.
# 2. 'CreatedDate': Set to the current timestamp, representing when the record was created.
# 3. 'ModifiedDate': Set to the current timestamp, representing when the record was last modified.
SourceDf = SourceDf.withColumn("IndCurrent", F.lit(1)) \
    .withColumn("CreatedDate", F.current_timestamp()) \
    .withColumn("ModifiedDate", F.current_timestamp())

# COMMAND ----------

SourceDf.filter(col("degree_days_freezing") == "0").display()

# COMMAND ----------

# Before applying the SCD Type 1 merge, let's inspect the data in the target table for a specific degree_days_freezing
display(spark.sql("select * from workspace.target_table.sales where degree_days_freezing='0'"))

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col

# Static configuration
table_name = "workspace.target_table.sales"
key_column = "franchiseID"
timestamp_column = "ModifiedDate"
hash_column = "RowHash"
created_column = "CreatedDate"

# Reference Delta table
target_table = DeltaTable.forName(spark, table_name)

# Aliases
src = SourceDf.alias("src")
tgt = target_table.alias("tgt")

# Columns to update (exclude key, timestamp, and created date)
columns_to_update = [
    col_name for col_name in SourceDf.columns 
    if col_name not in [key_column, timestamp_column, created_column]
]

# Construct SET dictionary for update
set_dict = {col_name: col(f"src.{col_name}") for col_name in columns_to_update}
set_dict[timestamp_column] = current_timestamp()  # Add ModifiedDate explicitly

# Perform SCD Type 1 MERGE
tgt.merge(
    src,
    f"tgt.{key_column} = src.{key_column}"
).whenMatchedUpdate(
    condition=col(f"src.{hash_column}") != col(f"tgt.{hash_column}"),
    set=set_dict
).whenNotMatchedInsertAll().execute()