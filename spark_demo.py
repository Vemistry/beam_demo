"""
Apache Spark Pipeline Demo
===========================
Xử lý dữ liệu lớn từ 5 file CSV bằng Apache Beam
Tính toán: URL nào được truy cập nhiều, thời gian trung bình, max, min.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import (
    sum as spark_sum, count, avg,
    max as spark_max, min as spark_min, round as spark_round
)
import logging
import time
import psutil

# 1. Ém toàn bộ log hệ thống ở tầng Python và Py4J (chỉ giữ ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def format_spark_row(row):
    """Format row output giống hệt Apache Beam"""
    return (
        f"URL: {row['url']}\n"
        f"  - Tổng thời gian: {row['total_time']} phút\n"
        f"  - Số lần truy cập: {row['visit_count']}\n"
        f"  - Thời gian trung bình: {row['avg_time']} phút\n"
        f"  - Thời gian tối đa: {row['max_time']} phút\n"
        f"  - Thời gian tối thiểu: {row['min_time']} phút\n"
        f"--------------------------------------------------"
    )

def run_simple_spark_pipeline():
    """Simplified Spark Pipeline with resource tracking"""
    start_time = time.time()
    process = psutil.Process()

    # Khởi tạo SparkSession và tắt thanh tiến trình ở console
    spark = SparkSession.builder \
        .appName("Simple-Spark-Demo") \
        .master("local[*]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
        
    # 2. Tắt các log WARN/INFO của lõi Spark (chỉ giữ Lỗi nghiêm trọng)
    spark.sparkContext.setLogLevel("ERROR")

    # 3. Kỹ thuật Schema-on-Read: Định nghĩa khung xương dữ liệu từ đầu
    schema = StructType([
        StructField("url", StringType(), True),
        StructField("time_minutes", IntegerType(), True)
    ])

    # Đọc dữ liệu và ốp thẳng schema vào (bỏ header=False do mặc định là False)
    df = spark.read.csv(
        '/home/vemistry/beam-demo/data/bt1_data*.csv',
        schema=schema
    )

    # Gom nhóm và tính toán (Aggregation)
    stats_df = df.groupBy("url").agg(
        spark_sum("time_minutes").alias("total_time"),
        count("time_minutes").alias("visit_count"),
        spark_round(avg("time_minutes"), 2).alias("avg_time"),
        spark_max("time_minutes").alias("max_time"),
        spark_min("time_minutes").alias("min_time")
    )

    # Kéo kết quả từ Worker về Driver Node
    results = stats_df.collect()
    
    # In ra Terminal
    for row in results:
        print(format_spark_row(row))

    spark.stop()

    end_time = time.time()
    memory_usage = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    
    print(f"Execution Time: {end_time - start_time:.2f} seconds")
    print(f"Memory Usage: {memory_usage:.2f} MB")

if __name__ == '__main__':
    run_simple_spark_pipeline()