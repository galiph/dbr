// Databricks notebook source

case class MyCaseClass(key: String, group: String, value: Int, someints: Seq[Int], somemap: Map[String, Int])
val dataframe = sc.parallelize(Array(MyCaseClass("a", "vowels", 1, Array(1), Map("a" -> 1)),
  MyCaseClass("b", "consonants", 2, Array(2, 2), Map("b" -> 2)),
  MyCaseClass("c", "consonants", 3, Array(3, 3, 3), Map("c" -> 3)),
  MyCaseClass("d", "consonants", 4, Array(4, 4, 4, 4), Map("d" -> 4)),
  MyCaseClass("e", "vowels", 5, Array(5, 5, 5, 5, 5), Map("e" -> 5)))
).toDF()
// now write it to disk
dataframe.write.mode("overwrite").parquet("/tmp/testParquet")

// COMMAND ----------


val data = spark.read.parquet("/tmp/testParquet")

display(data)

// COMMAND ----------

// MAGIC %r 
// MAGIC library(SparkR)
// MAGIC
// MAGIC data <- read.df("/tmp/testParquet", "parquet")
// MAGIC
// MAGIC display(data)

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC data = spark.read.parquet("/tmp/testParquet")
// MAGIC
// MAGIC display(data)

// COMMAND ----------

// MAGIC %sql 
// MAGIC CREATE TABLE scalaTable
// MAGIC USING parquet
// MAGIC OPTIONS (path "/tmp/testParquet")

// COMMAND ----------

// MAGIC %sql SELECT * FROM scalaTable
