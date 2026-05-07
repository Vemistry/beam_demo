"""
Apache Spark Pipeline Demo
===========================
Xử lý dữ liệu lớn từ 5 file CSV bằng Apache Beam
Tính toán: URL nào được truy cập nhiều, thời gian trung bình, max, min.
"""

# Import các thư viện cần thiết
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import (
    sum as spark_sum, count, avg,
    max as spark_max, min as spark_min, round as spark_round
)
import logging
import time
import psutil

# Cấu hình log để chỉ hiển thị lỗi nghiêm trọng
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Hàm định dạng kết quả đầu ra giống Apache Beam
# Đây là bước định dạng dữ liệu để hiển thị dễ đọc hơn
def format_spark_row(row):
    return (
        f"URL: {row['url']}\n"
        f"  - Tổng thời gian: {row['total_time']} phút\n"
        f"  - Số lần truy cập: {row['visit_count']}\n"
        f"  - Thời gian trung bình: {row['avg_time']} phút\n"
        f"  - Thời gian tối đa: {row['max_time']} phút\n"
        f"  - Thời gian tối thiểu: {row['min_time']} phút\n"
        f"--------------------------------------------------"
    )

# Hàm chính chạy pipeline Spark
# Bước này khởi tạo SparkSession và cấu hình log
# Đọc dữ liệu từ các file CSV, xử lý và tính toán thống kê
def run_simple_spark_pipeline():
    start_time = time.time()
    process = psutil.Process()

    # Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("Simple-Spark-Demo") \
        .master("local[*]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

    # Tắt log không cần thiết của Spark
    spark.sparkContext.setLogLevel("ERROR")

    # Định nghĩa schema cho dữ liệu CSV
    schema = StructType([
        StructField("url", StringType(), True),
        StructField("time", IntegerType(), True)
    ])

    # Đọc dữ liệu từ thư mục data
    data = spark.read.csv("data/*.csv", schema=schema, header=True)

    # Tính toán thống kê: tổng, trung bình, tối đa, tối thiểu
    stats = data.groupBy("url").agg(
        spark_sum("time").alias("total_time"),
        count("time").alias("visit_count"),
        spark_round(avg("time"), 2).alias("avg_time"),
        spark_max("time").alias("max_time"),
        spark_min("time").alias("min_time")
    )

    # Hiển thị kết quả
    stats.show(truncate=False)

    # Định dạng và in kết quả
    for row in stats.collect():
        print(format_spark_row(row))

    # Theo dõi tài nguyên sử dụng
    elapsed_time = time.time() - start_time
    memory_usage = process.memory_info().rss / (1024 * 1024)
    print(f"Thời gian chạy: {elapsed_time:.2f} giây")
    print(f"Bộ nhớ sử dụng: {memory_usage:.2f} MB")

if __name__ == "__main__":
    run_simple_spark_pipeline()