import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

def query_user():
    start_time = time.time()
    spark = SparkSession.builder \
        .appName("QueryUser") \
        .getOrCreate()

    filepath = "/capstone-dataset/user_purchases"
    output_dir = "../parquets"

    try:
        user_read = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(filepath)

        user_read = user_read.withColumn("date", to_date(col("purchaseTime")))
        user_read = user_read.coalesce(5)
        user_read.write.partitionBy("date").parquet(output_dir, mode="append")

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")

    spark.stop()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processing completed in {elapsed_time:.2f} seconds.")


def read_parquet_files(parquet_dir, merge_schema=True):

    spark = SparkSession.builder \
        .appName("ReadParquetFiles") \
        .getOrCreate()

    if merge_schema:
        df = spark.read.option("mergeSchema", "true").parquet(parquet_dir)
    else:
        df = spark.read.parquet(parquet_dir)

    df.printSchema()

    df.show()

    return df


if __name__ == "__main__":
    query_user()

