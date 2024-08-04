# Databricks notebook source
catalog = "<catalog_name>"
schema = "<schema_name>"
table_name = "baby_names"
silver_table_name = "baby_names_prepared"
gold_table_name = "top_baby_names_2021"
path_table = catalog + "." + schema
print(path_table) # Show the complete path

# COMMAND ----------

df_raw = spark.read.table(f"{path_table}.{table_name}")
display(df_raw)

# COMMAND ----------

from pyspark.sql.functions import col, initcap, when

# Rename "Year" column to "Year_Of_Birth"
df_rename_year = df_raw.withColumnRenamed("Year", "Year_Of_Birth")

# Change the case of "First_Name" data to initial caps
df_init_caps = df_rename_year.withColumn("First_Name", initcap(col("First_Name").cast("string")))

# Update column values from "M" to "Male" and "F" to "Female"
df_baby_names_sex = df_init_caps.withColumn(
"Sex", 
    when(col("Sex") == "M", "Male")
    .when(col("Sex") == "F", "Female")
)

# Display the data
display(df_baby_names_sex)

# Save DataFrame to a table
df_baby_names_sex.write.mode("overwrite").saveAsTable(f"{path_table}.{silver_table_name}")


# COMMAND ----------

from pyspark.sql.functions import expr, sum, desc
from pyspark.sql import Window

# Count of names for entire state of New York by sex
df_baby_names_2021_grouped=(df_baby_names_sex
.filter(expr("Year_Of_Birth == 2021"))
.groupBy("Sex", "First_Name")
.agg(sum("Count").alias("Total_Count"))
.sort(desc("Total_Count")))

# Display data
display(df_baby_names_2021_grouped)

# Save DataFrame to a table
df_baby_names_2021_grouped.write.mode("overwrite").saveAsTable(f"{path_table}.{gold_table_name}")

