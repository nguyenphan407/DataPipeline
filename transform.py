import logging

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

class Transformation:
    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        logging.info("Transform data")
        df = df.filter(df.total_amount > 20000)
        return df