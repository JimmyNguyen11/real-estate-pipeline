# real-estate-pipeline

Phiên bản sử dụng: (Đây là của máy t, còn hệ thống bro build như nào thì cứ tạm giữ nguyên như vậy trước đã)
 Airflow: 2.6.1
 HDFS: 3.3.6
 MySQL: 8.0.39
 Python: 3.10
Trước khi chạy lưu ý:
1. Cài Chrome để crawl
2. Tạo 1 kết nối như sau trong MySQL: 
        host='localhost',
        user='root',
        password='123456',
        database='demo'  -  Đây là mẫu t dùng, bro cần sửa như máy bro đã cấu hình
3. Trong airflow, vào mục Admin -> Connections, tạo 2 connection như ảnh sau:
   ![image](https://github.com/user-attachments/assets/a4b37ba4-9fcc-43f7-8bf0-11e34d9c2ab8)
   ![image](https://github.com/user-attachments/assets/20ea01fe-6e0d-42c1-968b-07f9a98f02b3)

5. Import thư viện: các thư viện được import trong code có kiểu
   ```
   from airflow.example_dags.plugins.mysql_to_hdfs import MysqlToHdfsOperator
   ```
   Bro cần chỉnh lại phần airflow.example_dags để airflow trong hệ thống của bro nhận diện được.
6. Trong code sẽ có những đoạn kiểu 
    ```
   file_path = 'home/hoang/crawled_data.csv'
   ```
Tức là dùng dir của máy t để làm, bro cần chỉnh lại như máy bro. (Cứ search 'hoang' rồi sửa)
Tương tự 1 số phần dùng câu lệnh hdfs, cũng cần chỉnh như mày bro (Cứ search 'hdfs' rồi sửa)
Có 1 phần bro cũng phải chỉnh lại phiên bản java mà hệ thống dùng như này (trong plugins/hooks)
   ```
   os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
   ```
Còn 1 số chỗ nữa nhưng t không nhớ hết, ghi chạy gặp lỗi thì nhắn nhé.


    
