# Demo Xử Lý Dữ Liệu Lớn với Apache Beam và Apache Spark

Dự án này bao gồm hai tập lệnh Python minh họa cách xử lý dữ liệu lớn bằng Apache Beam và Apache Spark. Cả hai tập lệnh đều xử lý dữ liệu từ nhiều file CSV và tính toán các thống kê cho các URL, bao gồm tổng thời gian truy cập, thời gian trung bình, thời gian tối đa, thời gian tối thiểu và số lần truy cập.

## Cấu Trúc Dự Án

```
beam_demo_output.txt       # Kết quả đầu ra của pipeline Apache Beam
beam_demo.py               # Tập lệnh pipeline Apache Beam
spark_demo_output.txt      # Kết quả đầu ra của pipeline Apache Spark
spark_demo.py              # Tập lệnh pipeline Apache Spark
data/                      # Thư mục chứa các file CSV đầu vào
  bt1_data1.csv
  bt1_data2.csv
  bt1_data3.csv
  bt1_data4.csv
  bt1_data5.csv
```

## Yêu Cầu

- Python 3.8+
- Apache Beam
- Apache Spark
- psutil (để theo dõi tài nguyên)

Cài đặt các thư viện Python cần thiết bằng pip:

```bash
pip install apache-beam pyspark psutil
```

## Dữ Liệu Đầu Vào

Dữ liệu đầu vào bao gồm nhiều file CSV nằm trong thư mục `data/`. Mỗi file chứa hai cột:

- `url`: URL được truy cập.
- `time`: Thời gian truy cập (tính bằng phút).

Ví dụ:
```
url,time
http://example.com,30
https://example.org,45
```

## Pipeline Apache Beam

Pipeline Apache Beam được triển khai trong file `beam_demo.py`. Các bước thực hiện:

1. Đọc dữ liệu từ các file CSV.
2. Phân tích từng dòng thành các tuple `(url, time)`.
3. Nhóm dữ liệu theo URL.
4. Tính toán các thống kê cho mỗi URL (tổng thời gian, số lần truy cập, thời gian trung bình, tối đa, tối thiểu).
5. Ghi kết quả vào file `beam_demo_output.txt`.

### Chạy Pipeline Beam

Kích hoạt môi trường ảo và chạy tập lệnh:

```bash
source venv/bin/activate
python beam_demo.py
```

### Kết Quả Mẫu

```
URL: https://chat.zalo.me
  - Tổng thời gian: 18060941 phút
  - Số lần truy cập: 73844
  - Thời gian trung bình: 244.58 phút
  - Thời gian tối đa: 479 phút
  - Thời gian tối thiểu: 10 phút
--------------------------------------------------
```

## Pipeline Apache Spark

Pipeline Apache Spark được triển khai trong file `spark_demo.py`. Các bước thực hiện tương tự như pipeline Beam nhưng sử dụng API DataFrame của Spark.

### Chạy Pipeline Spark

Kích hoạt môi trường ảo và chạy tập lệnh:

```bash
source venv/bin/activate
python spark_demo.py
```

### Kết Quả Mẫu

```
URL: http://facebook.com
  - Tổng thời gian: 17979962 phút
  - Số lần truy cập: 73552
  - Thời gian trung bình: 244.45 phút
  - Thời gian tối đa: 479 phút
  - Thời gian tối thiểu: 10 phút
--------------------------------------------------
```

## Hiệu Suất

### Apache Beam
- Thời gian thực thi: 6.39 giây
- Bộ nhớ sử dụng: 169.14 MB

### Apache Spark
- Thời gian thực thi: 7.27 giây
- Bộ nhớ sử dụng: 65.64 MB

## So Sánh

| Tính Năng              | Apache Beam                  | Apache Spark                 |
|------------------------|------------------------------|------------------------------|
| Loại Framework         | Batch và streaming          | Batch và streaming          |
| Hỗ Trợ Ngôn Ngữ        | Python, Java, Go, v.v.      | Python, Scala, Java, v.v.   |
| Môi Trường Chạy        | Linh hoạt (local, cloud, v.v.)| Local, cluster-based        |
