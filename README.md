# real-estate-pipeline

Phiên bản sử dụng: 
 - Airflow: 2.6.1
 - HDFS: 3.3.6
 - MySQL: 8.0.39
 - Python: 3.10
 - Hive: 3.1.3
 - Superset: 4.1.1
 - Java: 8
   
Trước khi chạy lưu ý:
1. Cài Chrome để crawl
2. Tạo 1 kết nối như sau trong MySQL:
   - host='localhost',
   - user='root',
   - password='123456',
   - database='demo'  -  
4. Trong airflow, vào mục Admin -> Connections, tạo 2 connection như ảnh sau (có thể sẽ phải điều chỉnh cho giống với cấu hình của hệ thống, tuy nhiên bắt buộc phải giữ nguyên tên connection)
   ![image](https://github.com/user-attachments/assets/a4b37ba4-9fcc-43f7-8bf0-11e34d9c2ab8)
   password: '123456'
   ![image](https://github.com/user-attachments/assets/20ea01fe-6e0d-42c1-968b-07f9a98f02b3) 

5. Import thư viện: các thư viện được import trong code có kiểu
   ```
   from airflow.example_dags.plugins.mysql_to_hdfs import MysqlToHdfsOperator
   ```
6. Trong code sẽ có những đoạn kiểu 
    ```
   file_path = 'home/hoang/crawled_data.csv'
   ```
Update 07/12: Hệ thống k8s cần bổ sung thêm Apache Hive (để phục vụ cung cấp metastore cho spark thrift server để thực hiện các câu query sql)
- Apache Hive: 3.1.3, khi build nếu có 1 số chỗ cần config 
  - Bình thường khi cài đặt hive, sẽ lưu các file cần thiết vào một folder, tạm gọi là 'hive', khi cài đặt, trong 'hive' sẽ có 1 folder tên 'conf', trong folder này chứa các file phục vụ cho việc config Hive, mình sẽ cần chỉnh sửa thuộc tính của Hive trong các file đó, t có đẩy lên r, bro check thử xem khi build Hive trên k8s thì có thể làm thế nào để Hive trên k8s conf giống như Hive mà t đã cài.
  - Bên cạnh đó khi cài hive, cần phải bổ sung thêm 1 file jar cho Hive có tên là 'iceberg-hive-runtime-1.3.0.jar': file này phục vụ chạy các bảng Iceberg với Hive (trên máy t thì phải tải file này về từ mạng, đưa vào folder 'hive' để chạy được, bro xem k8s xử lý mấy cái file jar này như nào nhé)
 - Spark: Tương tự như Hive, khi t cài đặt spark thì cũng sẽ lưu các file+thư mục cần thiết vào folder 'spark', trong đó cũng có 1 folder tên 'conf' chứa các file phục vụ cho việc config Spark, t cũng đã đẩy lên, bro xem xem khi build với k8s thì mình chỉnh sửa các thuộc tính ấy kiểu gì.
   - Tương tự Hive, cần bổ sung thêm 1 file jar cho Spark tên là 'iceberg-spark-runtime-3.3_2.12-1.3.0.jar'. Version Spark t đang dùng: 3.3.0
  

-Tất cả các file config hệ thống trên K8s nằm trong folder kubernetes.


    
