import time
from pyspark.sql import SparkSession

HDFS_RAW          = "hdfs://192.168.5.39:9000/user/nordic/raw/nordic-raw"
HDFS_PARQUET      = "hdfs://192.168.5.39:9000/user/nordic/parquet"
HDFS_PARQUET_PART = "hdfs://192.168.5.39:9000/user/nordic/parquet_partitioned"

spark = (
    SparkSession.builder
    .appName("NordicETL")
    .config("spark.hadoop.fs.defaultFS",         "hdfs://192.168.5.39:9000")
    .config("spark.sql.shuffle.partitions",      "24")
    .config("spark.executor.memory",             "2g")
    .config("spark.executor.cores",              "2")
    .config("spark.driver.memory",               "2g")
    .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
    .config("spark.sql.files.openCostInBytes",   str(128 * 1024 * 1024))
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"Reading from : {HDFS_RAW}")

t0 = time.time()
df = (
    spark.read
    .option("mergeSchema", "false")
    .json(HDFS_RAW)
    .withColumnRenamed("id",               "work_id")
    .withColumnRenamed("publication_year", "year")
    .select("work_id", "year", "cited_by_count", "authorships", "topics")
    .coalesce(24)
)
print(f"Partitions   : {df.rdd.getNumPartitions()}")
print(f"Read time    : {round(time.time() - t0, 1)}s")

print(f"Writing unpartitioned Parquet to {HDFS_PARQUET}...")
t0 = time.time()
df.write.mode("overwrite").parquet(HDFS_PARQUET)
print(f"Done in      : {round(time.time() - t0, 1)}s")

print(f"Writing partitioned Parquet to {HDFS_PARQUET_PART}...")
t0 = time.time()
df.write.mode("overwrite").partitionBy("year").parquet(HDFS_PARQUET_PART)
print(f"Done in      : {round(time.time() - t0, 1)}s")

print("ETL COMPLETE")
print(f"Unpartitioned : {HDFS_PARQUET}")
print(f"Partitioned   : {HDFS_PARQUET_PART}")

spark.stop()
