import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
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

        print("Ingesting data")

        # Cấu hình thông tin kết nối JDBC đến PostgreSQL
        jdbc_url = "jdbc:postgresql://localhost:5432/car_sales"  # Ví dụ: jdbc:postgresql://postgres:5432/mydatabase
        connection_properties = {
            "user": "postgres",  # Ví dụ: "myuser"
            "password": "password",  # Ví dụ: "mypassword"
            "driver": "org.postgresql.Driver"
        }

        # Tên bảng trong PostgreSQL mà bạn muốn ingestion dữ liệu
        table_name = "orders"  # Thay bằng tên bảng thật

        # Đọc dữ liệu từ PostgreSQL
        df = self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

        # Thêm cột phân vùng dựa trên ngày chạy file
        df = df.withColumn("year", lit(current_year)) \
            .withColumn("month", lit(current_month)) \
            .withColumn("day", lit(current_day))

        # Kiểm tra schema và hiển thị vài dòng dữ liệu
        print("Schema của DataFrame sau khi thêm cột phân vùng:")
        df.printSchema()
        return df