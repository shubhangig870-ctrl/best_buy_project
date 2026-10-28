from config.config import RAW_TARGET, BRONZE_TARGET
from pyspark.sql.functions import col, when, expr, current_timestamp, year, month, dayofmonth, to_date

def bronze_raw_pipeline(spark,RAW_TARGET,BRONZE_TARGET):
    df = spark.read.csv(raw, header=True, inferSchema=True)
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("ingestion_date", to_date(col("ingestion_time")))
    df = df.withColumn("_is_quarantined", when(col("Order ID").isNull(), "True").otherwise("False")) \
           .withColumn("batch_id", expr("uuid()"))
    df = df.withColumn("file_name", col("_metadata.file_name")) \
           .withColumn("file_size", col("_metadata.file_size"))
    df = df.withColumn("year", year(col("ingestion_date"))) \
           .withColumn("month", month(col("ingestion_date"))) \
           .withColumn("day", dayofmonth(col("ingestion_date")))
    df.write.format("csv") \
        .option("header", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .save(f"{BRONZE_TARGET}")
    print("Data successfully written to bronze location")