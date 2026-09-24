import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ============================================================
# 1. READ GLUE JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_PATH",
        "BRONZE_PATH"
    ]
)

JOB_NAME = args["JOB_NAME"]
SOURCE_PATH = args["SOURCE_PATH"]
BRONZE_PATH = args["BRONZE_PATH"]


# ============================================================
# 2. CREATE SPARK / GLUE CONTEXT
# ============================================================

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(JOB_NAME, args)


# ============================================================
# 3. LOG JOB INFORMATION
# ============================================================

print("=" * 80)
print("BANKING BRONZE ETL STARTED")
print("=" * 80)

print(f"Job Name     : {JOB_NAME}")
print(f"Source Path  : {SOURCE_PATH}")
print(f"Bronze Path  : {BRONZE_PATH}")

print(f"Spark Version: {spark.version}")


# ============================================================
# 4. READ RAW DATA FROM S3
# ============================================================

print("Reading raw data from S3...")

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(SOURCE_PATH)
)


# ============================================================
# 5. DISPLAY SOURCE DATA
# ============================================================

print("Source record count:")

source_count = df_raw.count()

print(source_count)

df_raw.show(10, truncate=False)


# ============================================================
# 6. BRONZE TRANSFORMATION
# ============================================================
# Bronze should contain data very close to the source.
#
# We generally DO NOT perform:
#   - complex business logic
#   - aggregations
#   - joins
#   - business calculations
#   - heavy cleansing
#
# We mainly add technical metadata.


df_bronze = (
    df_raw

    # --------------------------------------------------------
    # Record ingestion timestamp
    # --------------------------------------------------------
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )

    # --------------------------------------------------------
    # Ingestion date
    # --------------------------------------------------------
    .withColumn(
        "ingestion_date",
        F.current_date()
    )

    # --------------------------------------------------------
    # Source system
    # --------------------------------------------------------
    .withColumn(
        "source_system",
        F.lit("banking_s3_landing")
    )

    # --------------------------------------------------------
    # Glue job name
    # --------------------------------------------------------
    .withColumn(
        "etl_job_name",
        F.lit(JOB_NAME)
    )
)


# ============================================================
# 7. REORDER / SELECT COLUMNS
# ============================================================

df_bronze = df_bronze.select(
    "*"
)


# ============================================================
# 8. DATA QUALITY CHECK
# ============================================================

bronze_count = df_bronze.count()

print(f"Bronze record count: {bronze_count}")


if bronze_count == 0:

    print("WARNING: Bronze dataset contains 0 records.")

else:

    print("Bronze data validation successful.")


# ============================================================
# 9. WRITE BRONZE DATA TO S3
# ============================================================

print("Writing Bronze data to S3...")

(
    df_bronze
    .write
    .mode("append")
    .format("parquet")
    .partitionBy("ingestion_date")
    .save(BRONZE_PATH)
)


# ============================================================
# 10. FINAL VALIDATION
# ============================================================

print("=" * 80)
print("BRONZE ETL COMPLETED SUCCESSFULLY")
print("=" * 80)

print(f"Records processed : {bronze_count}")
print(f"Bronze location   : {BRONZE_PATH}")


# ============================================================
# 11. COMMIT GLUE JOB
# ============================================================

job.commit()