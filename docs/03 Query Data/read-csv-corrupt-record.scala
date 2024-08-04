// Databricks notebook source
// MAGIC %md
// MAGIC # PERMISSIVE mode (default)

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = new StructType()
  .add("_c0",IntegerType,true)
  .add("carat",DoubleType,true)
  .add("cut",StringType,true)
  .add("color",StringType,true)
  .add("clarity",StringType,true)
  .add("depth",IntegerType,true) // The depth field is defined wrongly. The actual data contains floating point numbers, while the schema specifies an integer.
  .add("table",DoubleType,true)
  .add("price",IntegerType,true)
  .add("x",DoubleType,true)
  .add("y",DoubleType,true)
  .add("z",DoubleType,true)
  .add("_corrupt_record", StringType, true) // The schema contains a special column _corrupt_record, which does not exist in the data. This column captures rows that did not parse correctly.

val diamonds_with_wrong_schema = spark.read.format("csv")
  .option("header", "true")
  .schema(schema)
  .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

// COMMAND ----------

// The mistake in the user-specified schema causes any row with a non-integer value in the depth column to be nullified.
// There are some rows, where the value of depth is an integer e.g. 64.0. They are parsed and coverted successfully.
// The _currupt_record column shows the string with original row data, which helps find the issue. 
display(diamonds_with_wrong_schema)

// COMMAND ----------

// Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column (named _corrupt_record by default).
// For example: spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count() and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
// Instead, you can cache or save the parsed results and then send the same query.

val badRows = diamonds_with_wrong_schema.filter($"_corrupt_record".isNotNull)
badRows.cache()
val numBadRows = badRows.count()
badRows.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC # DROPMALFORMED mode

// COMMAND ----------

val diamonds_with_wrong_schema_drop_malformed = spark.read.format("csv")
  .option("mode", "DROPMALFORMED")
  .option("header", "true")
  .schema(schema)
  .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

// COMMAND ----------

display(diamonds_with_wrong_schema_drop_malformed)

// COMMAND ----------

// MAGIC %md
// MAGIC # FAILFAST mode

// COMMAND ----------

val diamonds_with_wrong_schema_fail_fast = spark.read.format("csv")
  .option("mode", "FAILFAST")
  .option("header", "true")
  .schema(schema)
  .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

// COMMAND ----------

display(diamonds_with_wrong_schema_fail_fast)
