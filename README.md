# Apache Beam vs Apache Spark - Demo Xử Lý Dữ Liệu Lớn

## 📊 Dự án

Demo xử lý dữ liệu lớn bằng **Apache Beam** và **Apache Spark**. So sánh cách hoạt động, hiệu suất, và khi nào dùng cái nào.

**Dữ liệu**: 5 file CSV chứa URL và thời gian truy cập (phút)

---

## 🚀 Chạy Demo

### Yêu cầu
```bash
Python 3.8+
apache-beam >= 2.50.0
pyspark >= 3.3.0
```

### Cài đặt
```bash
pip install apache-beam pyspark
```

### Chạy
```bash
# Apache Beam
python beam_demo.py

# Apache Spark  
python spark_demo.py
```

---

## 🔍 Apache Beam là gì? Làm sao biết nó là Beam?

### Định nghĩa đơn giản
**Apache Beam** = Framework để xử lý dữ liệu **theo luồng** (stream) hoặc **batch**, với API **thống nhất** (cùng code cho cả hai).

### Nhận biết Beam trong code - Những keyword chính:

#### 1. **`beam.Pipeline()`** - Định tuyến công việc
```python
with beam.Pipeline() as pipeline:  # ← ĐÂY LÀ BEAM
    result = (
        pipeline
        | 'Read' >> ReadFromText('file.csv')
        | 'Process' >> Map(some_function)
    )
```
- **Pipe operator `|`** - Xâu chuỗi transformations (chỉ có Beam)
- **`>>`** - Đặt tên cho mỗi bước (để debug dễ hơn)

#### 2. **`beam.ParDo()`** - Logic tuỳ chỉnh
```python
class ParseCSV(beam.DoFn):  # ← BeamDoFn
    def process(self, element):
        # xử lý từng element
        yield result
```
Đây là **đặc trưng của Beam** - ParDo tự động xử lý song song.

#### 3. **`beam.CombinePerKey()`** - Gom nhóm + tính toán
```python
data | beam.CombinePerKey(sum)  # Gom theo key + cộng value
```

#### 4. **`PipelineOptions`** - Cấu hình runner
```python
options = PipelineOptions(runner='DirectRunner')  # Local
# hoặc
options = PipelineOptions(runner='DataflowRunner')  # Google Cloud
```


---

## 📊 So sánh: Beam vs MapReduce

| Đặc tính | MapReduce (Hadoop) | Apache Beam |
|---------|-------------------|------------|
| **Mô hình** | Map → Shuffle → Reduce | DAG-based (tổng quát hơn) |
| **Code** | Phải viết Mapper & Reducer riêng | Viết 1 lần, chạy khắp nơi |
| **Latency** | Chậm (batch only) | Nhanh (streaming support) |
| **Flexible** | ❌ Khó mở rộng | ✅ Mở rộng dễ |
| **Runners** | ❌ Chỉ Hadoop | ✅ DirectRunner, Dataflow, Spark, Flink |
| **API** | ❌ Low-level (Java) | ✅ Python, Java, Go |
| **Ngữ cảnh** | 2010-2015 (cũ) | 2016-2026 (hiện đại) |

### Ví dụ so sánh

**MapReduce (Hadoop):**
```java
// Mapper.java
class WordMapper extends Mapper<...> {
    public void map(... context) {
        String word = value.toString();
        context.write(new Text(word), new IntWritable(1));
    }
}

// Reducer.java
class WordReducer extends Reducer<...> {
    public void reduce(... context) {
        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }
}

// main() - Phải config Job, InputFormat, OutputFormat...
```

**Apache Beam (Thay thế):**
```python
# 1 file thôi!
with beam.Pipeline() as pipeline:
    result = (
        pipeline
        | 'Read' >> ReadFromText('file.txt')
        | 'Parse' >> Map(lambda x: (x, 1))
        | 'Group' >> beam.CombinePerKey(sum)  # Thay Reduce
        | 'Write' >> WriteToText('output')
    )
# Chạy local hoặc cloud - cùng code!
```

**Tóm tắt:**
- MapReduce = **Cũ, phức tạp, chỉ batch**
- Beam = **Mới, đơn giản, batch + stream, đa runner**

---

## 🎯 Pipeline của Demo này

### Beam Pipeline (beam_demo.py)
```
Read 5 CSV files
    ↓
ParseCSV (DoFn) - tách URL, time
    ↓
CombinePerKey - gom theo URL
    ↓
CalculateStats (DoFn) - tính sum, avg, min, max
    ↓
Filter - lấy high traffic (> 1000 phút)
    ↓
Output kết quả
```

**Key Beam concepts:**
- `beam.ParDo()` → xử lý theo element
- `beam.CombinePerKey()` → gom + tính toán
- `beam.Filter()` → lọc dữ liệu

### Spark Pipeline (spark_demo.py)
```
Read 5 CSV files → DataFrame
    ↓
Cast type (String → Int)
    ↓
groupBy("url").agg() - gom + tính toán
    ↓
Filter, Sort
    ↓
Show results / SQL query
```

**Key Spark concepts:**
- DataFrame API (kiểu SQL table)
- `.agg()` với SQL functions
- Catalyst optimizer tự động

---

## � Kết quả Thực Tế

### ✅ Apache Beam Output

---

## 🔥 High Traffic URLs (> 1000 visits)

| URL | Total Time | Visits | Avg Time | Max | Min |
|-----|-----------:|-------:|---------:|----:|----:|
| http://facebook.com | 17,979,962 | 73,552 | 244.45 | 479 | 10 |
| https://open.spotify.com | 17,933,894 | 73,259 | 244.80 | 479 | 10 |
| https://youtube.com | 18,077,251 | 73,801 | 244.95 | 479 | 10 |



## 🏆 Top 5 URLs by Total Time

| Rank | URL | Total Time |
|-----:|-----|-----------:|
| 1 | https://tuoitre.vn | 18,142,631 |
| 2 | https://zingnews.vn | 18,124,592 |
| 3 | https://youtube.com | 18,077,251 |
| 4 | https://tinhte.vn | 18,071,893 |
| 5 | https://chat.zalo.me | 18,060,941 |

### ✅ Apache Spark Output



## 📊 Spark Pipeline - Statistics per URL

| URL | Total Time | Visit Count | Avg Time | Max Time | Min Time |
|-----|-----------:|------------:|---------:|---------:|---------:|
| https://tuoitre.vn | 18,142,631 | 74,117 | 244.78 | 479 | 10 |
| https://zingnews.vn | 18,124,592 | 74,261 | 244.07 | 479 | 10 |
| https://youtube.com | 18,077,251 | 73,801 | 244.95 | 479 | 10 |
| https://tinhte.vn | 18,071,893 | 74,067 | 243.99 | 479 | 10 |
| https://chat.zalo.me | 18,060,941 | 73,844 | 244.58 | 479 | 10 |



## 📈 Spark SQL - Top 5 Most Visited

| Rank | URL | Visits | Total Time |
|-----:|-----|-------:|-----------:|
| 1 | https://zingnews.vn | 74,261 | 18,124,592 |
| 2 | https://tuoitre.vn | 74,117 | 18,142,631 |
| 3 | https://tinhte.vn | 74,067 | 18,071,893 |
| 4 | https://chat.zalo.me | 73,844 | 18,060,941 |
| 5 | https://youtube.com | 73,801 | 18,077,251 |



## 🔥 Spark SQL - High Engagement (Avg Time per URL)

| URL | Avg Time |
|-----|---------:|
| https://www.instagram.com | 245.06 |
| https://youtube.com | 244.95 |
| https://open.spotify.com | 244.80 |
| https://tuoitre.vn | 244.78 |
| https://chat.zalo.me | 244.58 |
| http://facebook.com | 244.45 |
| https://zingnews.vn | 244.07 |
| https://tinhte.vn | 243.99 |

### 📊 Thống Kê Chung

- **Tổng records**: 590,000
- **Tổng thời gian**: 144,304,847 phút
- **Thời gian trung bình**: 244.58 phút/truy cập
- **Số URLs duy nhất**: 8
- **Thời gian tối đa**: 479 phút
- **Thời gian tối thiểu**: 10 phút

---

## 📈 Kết quả tính toán

Cả Beam và Spark đều tính:
- ✅ **Tổng thời gian** per URL
- ✅ **Số lần truy cập** 
- ✅ **Thời gian trung bình**
- ✅ **Min/Max time**
- ✅ **Top URLs** + **High traffic URLs**
- ✅ **Sorted results** theo tiêu chí khác nhau

---

## 🔄 So sánh: Apache Beam vs Apache Spark - Thực Hành

Dựa trên kết quả thực tế từ demo:

### 📌 Kết quả Tính Toán (Nhất quán)
**Cả Beam và Spark đều cho ra cùng kết quả:**
- URL #1: https://tuoitre.vn với 18,142,631 phút (74,117 lần truy cập)
- URL #2: https://zingnews.vn với 18,124,592 phút (74,261 lần truy cập)
- Thời gian trung bình: 244.58 phút/truy cập
- Tổng 590,000 records được xử lý chính xác

### 🎯 Phong cách Code & Cấu trúc

| Tiêu chí | Apache Beam | Apache Spark |
|---------|------------|-------------|
| **Phong cách** | Functional (Pipeline → ParDo → CombinePerKey) | Imperative/Declarative (DataFrame + SQL) |
| **Đơn vị xử lý** | Element-by-element (ParDo) | Batch/Distributed (DataFrame partitions) |
| **Tính linh hoạt** | DAG-based, dễ mở rộng | SQL-friendly, dễ optimize |
| **Debug** | Pipe operator `\|` + `>>` tên bước | Catalyst query optimizer tự động |

### 💼 Tình huống Sử Dụng (Từ Demo)

**Chọn Apache Beam khi:**
- ✅ Cần **xử lý streaming thời gian thực** (logs, IoT sensors, real-time ads)
- ✅ Chạy trên **Google Cloud Dataflow** 
- ✅ Muốn **code 1 lần, chạy ở DirectRunner, Spark Runner, Flink, Dataflow**
- ✅ Cần **windowing** (5-phút window, session window)
- ✅ Phức tạp về **transformations** (custom DoFn logic)

**Chọn Apache Spark khi:**
- ✅ Xử lý **batch data lớn** (TB+ data warehouse)
- ✅ Cần **SQL queries** (như demo: `SELECT url, SUM(time), COUNT(*) GROUP BY url`)
- ✅ **Machine Learning** (MLlib, MLflow integration)
- ✅ Chạy **on-premise** hoặc bare metal clusters
- ✅ Team quen với **SQL** hơn lập trình hàm

### 📊 Hiệu Năng (Từ Demo)

| Metric | Beam | Spark |
|--------|------|-------|
| **Records xử lý** | 590,000 ✅ | 590,000 ✅ |
| **Tốc độ tính toán** | Nhanh (ParDo + CombinePerKey) | Rất nhanh (Catalyst optimizer) |
| **Memory usage** | Thấp (element-based) | Trung bình (DataFrame optimization) |
| **Độ chính xác** | 100% ✅ | 100% ✅ |

**Nhận xét:** Demo này là batch processing (không streaming), nên Spark có lợi thế về Catalyst optimizer. Nhưng Beam vẫn hoàn thành tốt và có lợi nếu sau này cần streaming.

---

## 🔄 Beam vs Spark - Chọn cái nào?

### Chọn **Apache Beam** khi:
✅ Cần xử lý **streaming real-time** (< 1 giây latency)  
✅ Muốn **code 1 lần, chạy khắp** (DirectRunner, Dataflow, Spark, Flink)  
✅ Chạy trên **Google Cloud Dataflow**  
✅ Cần **windowing** (time windows, session windows)  
✅ Pipeline phức tạp với **custom logic** (complex transformations)

### Chọn **Apache Spark** khi:
✅ Xử lý **batch data lớn** (TB+)  
✅ Cần **SQL queries**  
✅ Có **Machine Learning** cần (MLlib)  
✅ Chạy **on-premise** (Hadoop cluster, Kubernetes)  
✅ Team quen với **SQL** hơn functional programming  

---

## 💡 Lợi ích của Beam so với MapReduce

| Vấn đề | MapReduce | Beam |
|--------|-----------|------|
| **Code lặp lại** | Mapper + Reducer riêng cho mỗi job | Viết 1 lần, chạy mọi nơi |
| **Latency** | Phút ~ giờ | Millisecond ~ giây |
| **Dữ liệu streaming** | ❌ Không support | ✅ Native support |
| **Tính linh hoạt** | ❌ Khó mở rộng | ✅ DAG linh hoạt |
| **Infrastructure** | Chỉ Hadoop | Dataflow, Spark, Flink, ... |

**Kết luận:** Beam là **phiên bản hiện đại và toàn diện** của MapReduce.

---

## 🚀 Thực hành

### 1. Chạy Beam
```bash
python beam_demo.py
```
Chú ý:
- Dòng `[STEP 1], [STEP 2]...` = Các bước của pipeline
- Output JSON có keys: `url, total_time, visit_count, avg_time, max_time, min_time`

### 2. Chạy Spark
```bash
python spark_demo.py
```
Chú ý:
- DataFrame `.show()` hiển thị dạng bảng (SQL style)
- `cast("int")` = type conversion
- `groupBy().agg()` = SQL GROUP BY

### 3. So sánh
- **Beam**: Pipe operator `|`, ParDo(), CombinePerKey()
- **Spark**: DataFrame, SQL-like syntax
- **Cùng kết quả** nhưng cách viết khác nhau

---

## � File Output

Sau khi chạy, các file output được lưu:
- **beam_demo_output.txt** - Output chi tiết từ Apache Beam
- **spark_demo_output.txt** - Output chi tiết từ Apache Spark

Các file này chứa:
- Log và messages từ quá trình xử lý
- Kết quả các bước (STEP 1, STEP 2, ...)
- DataFrame/table output 
- Top URLs và High traffic URLs
- Statistics tổng hợp

---

## �📚 Tài liệu

- **Apache Beam**: https://beam.apache.org/
- **Apache Spark**: https://spark.apache.org/
- **MapReduce (lịch sử)**: https://en.wikipedia.org/wiki/MapReduce
