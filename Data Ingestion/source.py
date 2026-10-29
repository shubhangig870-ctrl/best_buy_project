# Extract latest data from S3 into Databricks using PySpark
# ------------------------------------------------------------------
# Prerequisites:
#   1. Configure an S3 connection / instance profile or IAM role
#      in Databricks (Unity Catalog > External locations / Storage
#      credentials) so the cluster can read the bucket.
#   2. Set the AWS region and bucket name below.
# ------------------------------------------------------------------

from pyspark.dbutils import DBUtils
from pyspark.sql.functions import lit, col, max as spark_max, current_date
import re

from pyspark.sql.functions import lit, col, max as spark_max
from config.config import RAW_TARGET , SOURCE

print(f"Reading from: {SOURCE}")
def source_pipeline(spark):
    dbutils = DBUtils(spark)
    #self.SOURCE = SOURCE
    # print(s3_path)
    # Use dbutils to list files and find the latest
    files = dbutils.fs.ls(SOURCE)
    latest_file = sorted(files, key=lambda x: x.modificationTime, reverse=True)[0]
    # 1. Read the full dataset from S3
    df = (
        spark.read
        .format('csv')
        .option("header", "true")
        .option("inferSchema", "true")
        .load(latest_file.path)
    )

    print(f"Total rows loaded from S3: {df.count()}")
    df.show(10)

    latest_df = df.withColumn("ingestion_date", current_date())

    print("Added 'ingestion_date' column with current date")
    latest_df.display()

    # 2. Write the enriched data to the raw destination
    print(f"Writing to: {RAW_TARGET}")
    (
        latest_df.write
        .format("csv")
        .mode("overwrite")
        .option("header","true")
        .option("inferSchema","true")
        .option("mergeSchema", "true")
        .save(RAW_TARGET)
    )
    print("Data successfully written to RAW_TARGET location")

