# Databricks notebook source
main = "databricks_ws_702ccf4e_1398_4d08_9528_86267651a25f"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Set Up the Environment

# COMMAND ----------

# Create a target Delta table
target_data = [
    (1, 'Alice', 34),
    (2, 'Bob', 45),
    (3, 'Charlie', 29)
]

target_df = spark.createDataFrame(target_data, ["id", "name", "age"])
target_df.write.format("delta").mode("overwrite").save("/tmp/target_table")
target_df.writeTo(f"{main}.default.target_table").createOrReplace()


# Create a source Delta table
source_data = [
    (1, 'Alice', 35),  # Alice's age is updated
    (4, 'David', 23)   # David is a new record
]

source_df = spark.createDataFrame(source_data, ["id", "name", "age"])
source_df.writeTo(f"{main}.default.source_table").createOrReplace()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Perform the Merge Operation
# MAGIC

# COMMAND ----------

from delta.tables import *

# Load the Delta tables
target_table = DeltaTable.forName(spark, f"{main}.default.target_table")
source_table = DeltaTable.forName(spark, f"{main}.default.source_table").toDF()

# Perform the merge operation
target_table.alias("target").merge(
    source_table.alias("source"),
    "target.id = source.id"  # Join condition
).whenMatchedUpdate(
    set={
        "name": "source.name",
        "age": "source.age"
    }
).whenNotMatchedInsert(
    values={
        "id": "source.id",
        "name": "source.name",
        "age": "source.age"
    }
).whenNotMatchedBySourceDelete(
    condition="target.age > 30"  # Delete target records not in the source where age > 30
).execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Verify the Results

# COMMAND ----------

# Read the target table to see the results
updated_target_df = spark.read.table(f"{main}.default.target_table")
updated_target_df.display()

