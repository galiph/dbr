# Databricks notebook source
catalog = "<catalog_name>"
schema = "<schema_name>"
volume = "<volume_name>"
file_name = "new_baby_names.csv"
table_name = "baby_names"
path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume
path_table = catalog + "." + schema
print(path_table) # Show the complete path
print(path_volume) # Show the complete path

# COMMAND ----------

data = [[2022, "CARL", "Albany", "M", 42]]

df = spark.createDataFrame(data, schema="Year int, First_Name STRING, County STRING, Sex STRING, Count int")
# display(df)
(df.coalesce(1)
   .write
   .option("header", "true")
   .mode("overwrite")
   .csv(f"{path_volume}/{file_name}"))


# COMMAND ----------

df1 = spark.read.csv(f"{path_volume}/{file_name}",
    header=True,
    inferSchema=True,
    sep=",")
display(df1)


# COMMAND ----------

df1.write.mode("append").insertInto(f"{path_table}.{table_name}")
display(spark.sql(f"SELECT * FROM {path_table}.{table_name} WHERE Year = 2022"))
