// Databricks notebook source
spark

// COMMAND ----------

val salary_csv=spark.read.option("header","true").csv("/FileStore/tables/Salaries.csv")

// COMMAND ----------

salary_csv.write.format("iceberg").save("/FileStore/tables/salary_iceberg")

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/salary_iceberg

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/tables/salary_iceberg/data/

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/tables/salary_iceberg/metadata/

// COMMAND ----------

val metadata_avro=spark.read.format("avro").load("dbfs:/FileStore/tables/salary_iceberg/metadata/9e61a998-29a3-435c-a242-800802335d8e-m0.avro")

// COMMAND ----------

display(metadata_avro)

// COMMAND ----------

val metadata_snap=spark.read.format("avro").load("dbfs:/FileStore/tables/salary_iceberg/metadata/snap-3032019307583392698-1-9e61a998-29a3-435c-a242-800802335d8e.avro")

// COMMAND ----------

display(metadata_snap)

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/tables/salary_iceberg/metadata/v1.metadata.json

// COMMAND ----------

val salary_csv1=spark.read.option("header","true").csv("/FileStore/tables/Salaries.csv")

// COMMAND ----------

salary_csv1.write.mode("append").format("iceberg").save("/FileStore/tables/salary_iceberg")

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/tables/salary_iceberg/metadata/

// COMMAND ----------

val finaldf=spark.read.format("iceberg").load("/FileStore/tables/salary_iceberg")

// COMMAND ----------

finaldf.

// COMMAND ----------


