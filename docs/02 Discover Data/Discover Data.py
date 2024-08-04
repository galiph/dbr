# Databricks notebook source
# MAGIC %pip install bamboolib

# COMMAND ----------

import bamboolib as bam

# COMMAND ----------

bam

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))


# COMMAND ----------

f = open('/databricks-datasets/README.md', 'r')
print(f.read())


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.people10m OPTIONS (PATH 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta')
# MAGIC

# COMMAND ----------


