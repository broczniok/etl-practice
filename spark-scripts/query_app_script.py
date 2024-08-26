from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
import time


def query_app():
    start_time = time.time()
    spark = SparkSession.builder \
        .appName("QueryApp") \
        .getOrCreate()

    filepath = "/capstone-dataset/mobile_app_clickstream"
    output_dir = "../parquets"

    try:
        app_read = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(filepath)

        app_read = app_read.withColumn("date", to_date(col("eventTime")))
        app_read = app_read.coalesce(5)
        app_read.write.partitionBy("date").parquet(output_dir, mode="append")

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")

    spark.stop()
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Processing completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    query_app()
