"""
Apache Spark Pipeline Demo
===========================
Fixed version - không lỗi column + chạy ổn định
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    max as spark_max, min as spark_min, round as spark_round
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_spark_pipeline():
    print("=" * 60)
    print("APACHE SPARK - BIG DATA PROCESSING DEMO")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("Beam-vs-Spark-Demo") \
        .master("local[*]") \
        .getOrCreate()

    print("\n[STEP 1] Reading data...")

    df = spark.read.csv(
        '/home/vemistry/beam-demo/data/bt1_data*.csv',
        header=False,
        inferSchema=False
    ).toDF("url", "time_minutes")

    # cast an toàn
    df = df.withColumn("time_minutes", col("time_minutes").cast("int"))

    print(f"Total records loaded: {df.count()}")

    print("\n[STEP 2] Sample data:")
    df.show(5)

    print("\n[STEP 3] Statistics per URL:")

    stats_df = df.groupBy("url").agg(
        spark_sum("time_minutes").alias("total_time"),
        count("time_minutes").alias("visit_count"),
        avg("time_minutes").alias("avg_time"),
        spark_max("time_minutes").alias("max_time"),
        spark_min("time_minutes").alias("min_time")
    )

    stats_df = stats_df.withColumn(
        "avg_time",
        spark_round(col("avg_time"), 2)
    ).orderBy(col("total_time").desc())

    stats_df.show(truncate=False)

    print("\n[STEP 4] High traffic (>1000):")
    high_traffic_df = stats_df.filter(col("total_time") > 1000)
    high_traffic_df.show(truncate=False)

    print("\n[STEP 5] Top 5 URLs:")
    top5 = stats_df.limit(5).collect()

    for i, row in enumerate(top5, 1):
        print(f"#{i}: {row['url']} - {row['total_time']} phút")

    print("\n[STEP 6] Overall stats:")

    total_records = df.count()
    total_time = df.agg(spark_sum("time_minutes")).collect()[0][0]
    avg_time = df.agg(avg("time_minutes")).collect()[0][0]
    unique_urls = df.select("url").distinct().count()

    print(f"- Records: {total_records}")
    print(f"- Total time: {total_time}")
    print(f"- Avg time: {avg_time:.2f}")
    print(f"- Unique URLs: {unique_urls}")

    spark.stop()
    print("\n DONE SPARK PIPELINE!")


def run_sql_pipeline():
    print("\n" + "=" * 60)
    print("SPARK SQL DEMO")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("Spark-SQL") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv(
        '/home/vemistry/beam-demo/data/bt1_data*.csv',
        header=False
    ).toDF("url", "time_minutes")

    df = df.withColumn("time_minutes", col("time_minutes").cast("int"))
    df.createOrReplaceTempView("visits")

    print("\nTop 5 most visited:")
    spark.sql("""
        SELECT url,
               COUNT(*) as visits,
               SUM(time_minutes) as total_time
        FROM visits
        GROUP BY url
        ORDER BY visits DESC
        LIMIT 5
    """).show(truncate=False)

    print("\nHigh engagement:")
    spark.sql("""
        SELECT url,
               AVG(time_minutes) as avg_time
        FROM visits
        GROUP BY url
        HAVING AVG(time_minutes) > 200
        ORDER BY avg_time DESC
    """).show(truncate=False)

    spark.stop()
    print("\n DONE SQL PIPELINE!")


if __name__ == "__main__":
    try:
        run_spark_pipeline()
        run_sql_pipeline()
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()