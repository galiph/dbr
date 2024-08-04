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

