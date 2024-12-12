CREATE DATABASE IF NOT EXISTS iceberg.bds4_warehouse;

-- DROP TABLE IF EXISTS iceberg.bds3_warehouse.demo purge;

CREATE TABLE IF NOT EXISTS iceberg.bds4_warehouse.demo1 (
    du_an STRING,
    price_VND DOUBLE,
    status STRING,
    area DOUBLE,
    price_m2 DOUBLE,
    toilet BIGINT,
    room BIGINT,
    doc STRING,
    type STRING,
    So_tang BIGINT,
    furnishing STRING,
    phuong STRING,
    quan STRING,
    created_date TIMESTAMP,
    total DOUBLE
) USING iceberg
LOCATION 'hdfs:///demo4/warehouse/demo1';

INSERT INTO iceberg.bds4_warehouse.demo1
SELECT
    du_an,
    price_VND,
    status,
    area,
    price_m2,
    toilet,
    room,
    doc,
    type,
    So_tang,
    furnishing,
    -- Sử dụng REGEXP_EXTRACT để trích xuất thông tin phường và quận
    REGEXP_EXTRACT(location, 'Phường ([^,]+)', 1) AS phuong,
    REGEXP_EXTRACT(location, 'Quận ([^,]+)', 1) AS quan,
    created_date,
    ROUND((area * price_m2) / 1000, 2) AS total
FROM iceberg.bds4_staging.demo1;
