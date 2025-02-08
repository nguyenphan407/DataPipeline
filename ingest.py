import logging

import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
from py4j.java_gateway import java_import
import datetime

class Ingestion:
    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        # Lấy ngày hiện tại
        current_date = datetime.datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        current_day = current_date.day

        logging.info("Ingesting data")

        # Cấu hình thông tin kết nối JDBC đến PostgreSQL
        jdbc_url = "jdbc:postgresql://localhost:5432/car_sales"  # Ví dụ: jdbc:postgresql://postgres:5432/mydatabase
        connection_properties = {
            "user": "postgres",  # Ví dụ: "myuser"
            "password": "password",  # Ví dụ: "mypassword"
            "driver": "org.postgresql.Driver"
        }

        conf = self.spark._jsc.hadoopConfiguration()
        java_import(self.spark._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")
        conf.set("fs.defaultFS", "hdfs://localhost:9000")
        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(conf)
        exists = fs.exists(self.spark._jvm.org.apache.hadoop.fs.Path("hdfs://localhost:9000/datalake/orders"))
        tblQuerry = ""
        if (exists):
            df = self.spark.read.parquet("hdfs://localhost:9000/datalake/orders")
            tblQuerry = f"(SELECT * FROM orders WHERE order_id > {df.agg({'order_id': 'max'}).head()[0]})"
        else:
            tblQuerry = "(SELECT * FROM orders)"

        print(exists)
        print(tblQuerry)
        # Đọc dữ liệu từ PostgreSQL
        order_df = self.spark.read.jdbc(url=jdbc_url, table=tblQuerry, properties=connection_properties)
        # Thêm cột phân vùng dựa trên ngày chạy file
        order_df = order_df.withColumn("year", lit(current_year)) \
            .withColumn("month", lit(current_month)) \
            .withColumn("day", lit(current_day))

        # Kiểm tra schema và hiển thị vài dòng dữ liệu
        print("Schema của DataFrame sau khi thêm cột phân vùng:")
        order_df.printSchema()
        return order_df