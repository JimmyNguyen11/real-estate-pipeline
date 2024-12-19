# real-estate-pipeline

# Phiên bản sử dụng: 
 - Airflow: 2.6.1
 - HDFS: 3.3.6
 - MySQL: 8.0.39
 - Python: 3.10
 - Hive: 3.1.3
 - Superset: 4.1.1
 - Java: 8

# Giới thiệu Project
Project này thực hiện nhằm xây dựng 1 data pipeline để thực hiện vận chuyển, lưu trữ dữ liệu BĐS được crawl từng ngày từ web Nhatot và trực quan hóa dữ liệu thu thập được trên Superset.

# Nhiệm vụ từng thư mục
1. dags/subdags: lưu subdags để phục vụ dags chính (dags chính nằm ở scripts/etl.py)
2. data: lưu trữ data mẫu (/data/crawled_data_1.csv phục vụ việc test) và lưu các câu lệnh sql để xử lý dữ liệu
3. kubernetes: lưu trữ các file config hệ thống
4. scripts: lưu trữ các code python để phục vụ ETL

# Trước khi chạy lưu ý:
1. Cài Chrome (phục vụ crawl)
2. Tạo 1 kết nối như sau trong MySQL:
   - host='localhost',
   - user='root',
   - password='123456',
   - database='demo'
3. Thông tin các dịch vụ:
   - Airflow: localhost:8085
   - HDFS: localhost:9870
   - Spark Thrift Server: localhost:4040
   - Superset: localhost:8088  


    
