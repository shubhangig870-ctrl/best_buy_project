from config.config import BRONZE_TARGET

def silver_cleaning_pipeline(spark,BRONZE_TARGET):
    from pyspark.sql.functions import col,when

    from datetime import datetime, timedelta
    # Get yesterday's date
    now = datetime.now() - timedelta(days=0)     
    # yr, mon, dt = now.year, now.month, now.day
    year = now.year
    month = now.month
    day = now.day
    # Build dynamic S3 path - read from table root, let Delta handle partitions
    file_path = f"{bronze_path}year={year}/month={month}/day={day}/"

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    """Normalize column names: trim spaces, replace spaces and '-' with '_', lowercase all letters.
    Example: ' Order-ID ' -> 'order_id'
    """
    dfs = df
    for col_name in df.columns:
        clean_name = (
            col_name
            .strip()                    # remove leading/trailing spaces
            .replace(' ', '_')          # 'order id' -> 'order_id'
            .replace('-', '_')          # 'order-id' -> 'order_id'
            .lower()                    # 'Order_ID' -> 'order_id'
        )
        dfs = dfs.withColumnRenamed(col_name, clean_name)
    df = dfs.withColumn("courier_status",when(col("courier_status").isNull(),"pending").otherwise(col("courier_status")))
    from pyspark.sql.functions import col, upper

    df = df.withColumn("courier_status", upper(col("courier_status")))

    ## currency_nulls
    df = df.fillna({"currency": "not provide"})
    ## Amount
    from pyspark.sql.functions import col, when, abs

    df = df.withColumn(
        "Amount",
        when(col("Amount").isNull(), 0)  # replace nulls with 0
        .otherwise(col("Amount").cast("double"))  # cast to numeric
    )

    df = df.withColumn(
        "Amount",
        abs(col("Amount"))  # take absolute value
    )
    ## ship_city
    df = df.fillna({"ship_city": "not provide"})
    df = df.withColumn("ship_city", upper(col("ship_city")))

    ## ship_state
    df = df.fillna({"ship_state": "not provide"})
    df = df.withColumn("ship_state", upper(col("ship_state")))
    from pyspark.sql.functions import col, when

    df = df.withColumn(
        "ship_state",
        when(col("ship_state") == "RJ", "RAJASTHAN")
        .when(col("ship_state") == "PB", "PUNJAB")
        .when(col("ship_state") == "NL", "NAGALAND")
        .when(col("ship_state") == "AR", "ARUNACHAL PRADESH")
        .when(col("ship_state") == "RAJSHTHAN", "RAJASTHAN")
        .when(col("ship_state") == "RAJSTHAN", "RAJASTHAN")
        .when(col("ship_state") == "PUNJAB/MOHALI/ZIRAKPUR", "PUNJAB")
        .when(col("ship_state") == "APO", "ARUNACHAL PRADESH")
        .otherwise(col("ship_state"))
    )

    ## ship_postal_code
    df = df.fillna({"ship_postal_code":0})
    ## ship_country
    df = df.fillna({"ship_country":"IN"})
    ## promotion_ids
    df=df.withColumn(
        "has_promotion",col("promotion_ids").isNotNull()).withColumn(
        "promotion_type",when(col("promotion_ids").contains("Free-Financing"),"Free-Financing").when(col("promotion_ids").contains("Free Shipping"),"Free Shipping").when(col("promotion_ids").isNotNull(),"Other promotion").otherwise("No promotion"))

    df = df.drop("promotion_ids")

    ## fulfilled_by
    df = df.fillna({"fulfilled_by":"NOT provide"})

    ## Unnamed: 22
    df = df.drop("unnamed:_22")
    df = df.withColumn("category",upper(col("category")))

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    # Path to your Delta table in S3

    silver_path = f"s3://destiny-buck-amazon/silver_clean/"
        # Check if table exists (verify it is actually loadable, not just a stranded _delta_log)
    try:
        spark.sql(f"DESCRIBE DETAIL delta.`{silver_path}`")
        silver_exists = True
    except Exception:
        silver_exists = False
    if silver_exists:
            # Load existing Delta table
            delta_silver = DeltaTable.forPath(spark, silver_path)

            # Perform merge (upsert)
            (
                delta_silver.alias("target")
                .merge(
                    df.alias("source"),
                    "target.index = source.index"  # <-- replace with your join condition
                )
                .whenMatchedUpdateAll()   # update all columns if matched
                .whenNotMatchedInsertAll() # insert new rows if not matched
                .execute()
            )
    else:
            # If table doesn't exist, just write it
            df.write.format("delta") \
                .option("mergeSchema", "true") \
                .option("overwriteSchema", "true") \
                .mode("overwrite") \
                .save(silver_path)
    print("Data successfully written to silver location")