# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType

filePath = "dbfs:/FileStore/tables/Files/actors.csv"

schema = StructType([
    StructField("imdb_title_id", StringType()),
    StructField("ordering", IntegerType()),
    StructField("imdb_name_id", StringType()),
    StructField("category", StringType()),
    StructField("job", StringType()),
    StructField("characters", StringType()),
])

actorsDf = spark.read.format("csv") \
            .option("header","true") \
            .schema(schema) \
            .load(filePath)

# COMMAND ----------

actorsDf.write.mode("overwrite").json("dbfs:/FileStore/tables/Files/actors.json")

# COMMAND ----------

spark.read.format('csv').option('mode','PERMISSIVE').schema(schema).load(filePath)

spark.read.format('csv').option('mode','DROPMALFORMED').schema(schema).load(filePath)

spark.read.format('csv').option('mode','FAILFAST').schema(schema).load(filePath)

spark.read.format('csv').option('mode','FAILFAST').schema(schema).option("badRecordsPath", "/tmp/badRecordsPath").load(filePath)

# COMMAND ----------

outputFilePath = "dbfs:/FileStore/tables/Files/actors.parquet"


actorsDf.write.parquet(path=outputFilePath,mode='overwrite')

spark.read.schema(schema).option('enforceSchema',True).parquet(path=outputFilePath)