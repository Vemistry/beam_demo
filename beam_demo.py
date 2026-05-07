"""
Apache Beam Pipeline Demo
=========================
Xử lý dữ liệu lớn từ 5 file CSV bằng Apache Beam
Tính toán: URL nào được truy cập nhiều, thời gian trung bình, max, min.
"""

# Import các thư viện cần thiết
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import Map, ParDo
import logging
import time
import psutil

# Cấu hình log để chỉ hiển thị cảnh báo và lỗi
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Lớp xử lý từng dòng CSV
# Bước này chuyển đổi từng dòng CSV thành tuple (url, time)
class ParseCSV(beam.DoFn):
    def process(self, line):
        if line.strip():
            try:
                parts = line.split(',')
                url = parts[0].strip()
                time_minutes = int(parts[1].strip())
                yield (url, time_minutes)
            except (IndexError, ValueError) as e:
                logger.warning(f"Bỏ qua dòng không hợp lệ: {line} - Lỗi: {e}")

# Lớp tính toán thống kê
# Bước này tính tổng, trung bình, tối đa, tối thiểu cho mỗi URL
class CalculateStats(beam.DoFn):
    def process(self, element):
        url, times = element
        times_list = list(times)
        total_time = sum(times_list)
        count = len(times_list)
        avg_time = total_time / count if count > 0 else 0
        max_time = max(times_list)
        min_time = min(times_list)
        
        yield {
            'url': url,
            'total_time': total_time,
            'visit_count': count,
            'avg_time': round(avg_time, 2),
            'max_time': max_time,
            'min_time': min_time
        }

# Hàm chính chạy pipeline Beam
# Bước này định nghĩa pipeline và các bước xử lý
if __name__ == "__main__":
    start_time = time.time()
    process = psutil.Process()

    # Cấu hình pipeline
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            # Đọc dữ liệu từ các file CSV
            | "Đọc dữ liệu" >> ReadFromText("data/*.csv", skip_header_lines=1)
            # Phân tích từng dòng CSV
            | "Phân tích CSV" >> beam.ParDo(ParseCSV())
            # Nhóm dữ liệu theo URL
            | "Nhóm theo URL" >> beam.GroupByKey()
            # Tính toán thống kê
            | "Tính toán thống kê" >> beam.ParDo(CalculateStats())
            # Ghi kết quả ra file
            | "Ghi kết quả" >> WriteToText("beam_demo_output.txt")
        )

    # Theo dõi tài nguyên sử dụng
    elapsed_time = time.time() - start_time
    memory_usage = process.memory_info().rss / (1024 * 1024)
    print(f"Thời gian chạy: {elapsed_time:.2f} giây")
    print(f"Bộ nhớ sử dụng: {memory_usage:.2f} MB")