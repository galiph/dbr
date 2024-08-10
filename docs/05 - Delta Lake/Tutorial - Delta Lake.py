# Databricks notebook source
catalog = "databricks_ws_7618a276_40d3_44c3_ab50_37f0bc95541e"
path = "/Volumes/databricks_ws_7618a276_40d3_44c3_ab50_37f0bc95541e/default/ghema"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("firstName", StringType(), True),
  StructField("middleName", StringType(), True),
  StructField("lastName", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("birthDate", TimestampType(), True),
  StructField("ssn", StringType(), True),
  StructField("salary", IntegerType(), True)
])

df = spark.read.format("csv").option("header", True).schema(schema).load(f"{path}/export.csv")

# Create the table if it does not exist. Otherwise, replace the existing table.
df.writeTo("databricks_ws_7618a276_40d3_44c3_ab50_37f0bc95541e.default.people_10m").createOrReplace()

# If you know the table does not already exist, you can call this instead:
# df.saveAsTable("main.default.people_10m")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_ws_7618a276_40d3_44c3_ab50_37f0bc95541e.default.people_10m
# MAGIC

# COMMAND ----------

main = 'databricks_ws_7618a276_40d3_44c3_ab50_37f0bc95541e'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE databricks_ws_7618a276_40d3_44c3_ab50_37f0bc95541e.default.people_10m_prod LIKE databricks_ws_7618a276_40d3_44c3_ab50_37f0bc95541e.default.people_10m

# COMMAND ----------

from delta.tables import *



# COMMAND ----------

DeltaTable.createIfNotExists(spark).tableName(f"{main}.default.people_10m")\
    .addColumn("id", "INT")\
    .addColumn("firstName", "STRING")\
    .addColumn("middleName", "STRING")\
    .addColumn("lastName", "STRING", comment = "surname")\
    .addColumn("gender", "STRING")\
    .addColumn("birthDate", "TIMESTAMP")\
    .addColumn("ssn", "STRING")\
    .addColumn("salary", "INT")\
    .execute()


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("firstName", StringType(), True),
  StructField("middleName", StringType(), True),
  StructField("lastName", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("birthDate", DateType(), True),
  StructField("ssn", StringType(), True),
  StructField("salary", IntegerType(), True)
])

data = [
  (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', date.fromisoformat('1992-09-17'), '953-38-9452', 55250),
  (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', date.fromisoformat('1984-05-22'), '906-51-2137', 48500),
  (10000000, 'Joshua', 'Chas', 'Broggio', 'M', date.fromisoformat('1968-07-22'), '988-61-6247', 90000),
  (20000001, 'John', '', 'Doe', 'M', date.fromisoformat('1978-01-14'), '345-67-8901', 55500),
  (20000002, 'Mary', '', 'Smith', 'F', date.fromisoformat('1982-10-29'), '456-78-9012', 98250),
  (20000003, 'Jane', '', 'Doe', 'F', date.fromisoformat('1981-06-25'), '567-89-0123', 89900)
]

people_10m_updates = spark.createDataFrame(data, schema)
people_10m_updates.createTempView("people_10m_updates")

# ...

# COMMAND ----------


from delta.tables import DeltaTable

deltaTable = DeltaTable.forName(spark, f'{main}.default.people_10m')

(deltaTable.alias("people_10m")
  .merge(
    people_10m_updates.alias("people_10m_updates"),
    "people_10m.id = people_10m_updates.id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
)

# COMMAND ----------

df = spark.read.table(f"{main}.default.people_10m")
df_filtered = df.filter(df["id"] >= 9999998)
display(df_filtered)

