from Data Ingestion.source import source_pipeline
# source pipeline
source_pipeline(spark)

# bronze pipeline
from config.config import RAW_TARGET,BRONZE_TARGET
bronze_raw_pipeline(spark,RAW_TARGET,BRONZE_TARGET)