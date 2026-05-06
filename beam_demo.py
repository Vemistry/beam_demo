"""
Apache Beam Pipeline Demo
=========================
Xử lý dữ liệu lớn từ 5 file CSV bằng Apache Beam
Tính toán: URL nào được truy cập nhiều, thời gian trung bình, etc.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import Map, FlatMap, CombinePerKey, Filter, ParDo
from apache_beam.transforms.combiners import Top
import logging
import glob
from collections import defaultdict

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
                logger.warning(f"Error parsing line: {line}, Error: {e}")


class CalculateStats(beam.DoFn):
    """Calculate statistics for each URL"""
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
    )


class FilterHighTraffic(beam.DoFn):
    """Filter URLs with high traffic (> 1000 minutes)"""
    def process(self, element):
        url, times = element
        total_time = sum(times)
        if total_time > 1000:
            yield (url, times)


def run_beam_pipeline():
    """Main Beam Pipeline"""
    
    print("=" * 60)
    print("APACHE BEAM - BIG DATA PROCESSING DEMO")
    print("=" * 60)
    
    # Pipeline options
    options = PipelineOptions(
        runner='DirectRunner',
        project='beam-demo',
    )
    
    with beam.Pipeline(options=options) as pipeline:
        # Step 1: Read all CSV files
        print("\n[STEP 1] Reading data from 5 CSV files...")
        files_pattern = '/home/vemistry/beam-demo/data/bt1_data*.csv'
        
        results = (
            pipeline
            | 'Read Files' >> ReadFromText(files_pattern)
            
            # Step 2: Parse CSV data
            | 'Parse CSV' >> ParDo(ParseCSV())
            
            # Step 3: Group by URL
            | 'Group by URL' >> beam.CombinePerKey(beam.combiners.ToListCombineFn())
            
            # Step 4: Calculate statistics
            | 'Calculate Stats' >> ParDo(CalculateStats())
        )
        
        # Step 5: Filter high traffic URLs (> 1000 minutes)
        high_traffic = (
            results
            | 'Filter High Traffic' >> Filter(lambda x: x['total_time'] > 1000)
            | 'Format High Traffic' >> Map(lambda x: f"HIGH TRAFFIC: {x['url']} - {x['total_time']} phút")
        )
        
        # Step 6: All URLs stats
        all_stats = (
            results
            | 'Format All Stats' >> Map(format_output)
        )
        
        # Output
        all_stats | 'Print All Stats' >> Map(print)
        high_traffic | 'Print High Traffic' >> Map(print)
    
    print("\n[COMPLETED] Beam pipeline execution finished!")


def run_beam_pipeline_advanced():
    """Advanced Beam Pipeline with multiple transforms"""
    
    print("\n" + "=" * 60)
    print("APACHE BEAM - ADVANCED PIPELINE (Top URLs)")
    print("=" * 60)
    
    options = PipelineOptions(runner='DirectRunner')
    
    with beam.Pipeline(options=options) as pipeline:
        files_pattern = '/home/vemistry/beam-demo/data/bt1_data*.csv'
        
        # Step 1: Read and parse
        parsed_data = (
            pipeline
            | 'Read CSV' >> ReadFromText(files_pattern)
            | 'Parse Data' >> ParDo(ParseCSV())
        )
        
        # Step 2: Calculate total time per URL
        total_time_per_url = (
            parsed_data
            | 'Sum Time' >> beam.CombinePerKey(sum)
        )
        
        # Step 3: Count visits per URL  
        visit_count_per_url = (
            parsed_data
            | 'Count Visits' >> beam.CombinePerKey(beam.combiners.CountCombineFn())
        )
        
        # Step 4: Combine and sort (Top 5 URLs)
        top_urls = (
            total_time_per_url
            | 'To KV' >> Map(lambda x: (x[0], x[1]))
            | 'Top 5' >> Top.Of(5, key=lambda x: x[1])
            | 'Format Top' >> FlatMap(lambda x: [f"#{i+1}: {url} - {time} phút" 
                                                  for i, (url, time) in enumerate(x)])
        )
        
        top_urls | 'Print Top' >> Map(print)
    
    print("\n[COMPLETED] Advanced Beam pipeline execution finished!")


if __name__ == '__main__':
    print("\nStarting Apache Beam Demo...\n")
    
    try:
        run_beam_pipeline()
        run_beam_pipeline_advanced()
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n Demo completed!")
