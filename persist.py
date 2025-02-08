import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

class Persist:
    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df):
        print("Persisting")
        output_path = "hdfs://localhost:9000/datalake/orders"
        df.write.partitionBy("year", "month", "day").mode("append").parquet(output_path)