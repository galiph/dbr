# Databricks notebook source
catalog = "<catalog_name>"
schema = "<schema_name>"
volume = "<volume_name>"
download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
file_name = "baby_names.csv"
table_name = "baby_names"
path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume
path_table = catalog + "." + schema
print(path_table) # Show the complete path
print(path_volume) # Show the complete path

# COMMAND ----------

dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")

# COMMAND ----------

df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",")


# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumnRenamed("First Name", "First_Name")
df.printSchema

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")
