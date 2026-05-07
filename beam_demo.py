"""
Apache Beam Pipeline Demo
=========================
Xử lý dữ liệu lớn từ 5 file CSV bằng Apache Beam
Tính toán: URL nào được truy cập nhiều, thời gian trung bình, max, min.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import Map, ParDo
import logging
import time
import psutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParseCSV(beam.DoFn):
    """Parse CSV lines to (url, time) tuples"""
    def process(self, line):
        if line.strip():
            try:
                parts = line.split(',')
                url = parts[0].strip()
                time_minutes = int(parts[1].strip())
                yield (url, time_minutes)
            except (IndexError, ValueError) as e:
                logger.warning(f"Bỏ qua dòng không hợp lệ (có thể là header): {line} - Lỗi: {e}")

class CalculateStats(beam.DoFn):
    """Calculate statistics for each URL from grouped elements"""
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

def format_output(stats_dict):
    """Format statistics for output"""
    return (
        f"URL: {stats_dict['url']}\n"
        f"  - Tổng thời gian: {stats_dict['total_time']} phút\n"
        f"  - Số lần truy cập: {stats_dict['visit_count']}\n"
        f"  - Thời gian trung bình: {stats_dict['avg_time']} phút\n"
        f"  - Thời gian tối đa: {stats_dict['max_time']} phút\n"
        f"  - Thời gian tối thiểu: {stats_dict['min_time']} phút\n"
        f"--------------------------------------------------"
    )

def run_simple_beam_pipeline():
    """Simplified Beam Pipeline with resource tracking"""
    start_time = time.time()
    process = psutil.Process()
    options = PipelineOptions(runner='DirectRunner')

    with beam.Pipeline(options=options) as pipeline:
        results = (
            pipeline
            | 'Read Files' >> ReadFromText('/home/vemistry/beam-demo/data/bt1_data*.csv')
            | 'Parse CSV' >> ParDo(ParseCSV())
            
            | 'Group by URL' >> beam.GroupByKey()
            
            # Đưa vào hàm tính toán 
            | 'Calculate Statistics' >> ParDo(CalculateStats())
            
            # Format lại output thành chuỗi
            | 'Format Output' >> Map(format_output)
        )
        # In kết quả ra console (Local testing)
        results | 'Print Results' >> Map(print)

    end_time = time.time()
    memory_usage = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    print(f"Execution Time: {end_time - start_time:.2f} seconds")
    print(f"Memory Usage: {memory_usage:.2f} MB")

if __name__ == '__main__':
    run_simple_beam_pipeline()