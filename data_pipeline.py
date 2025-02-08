import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import ingest
import transform
import persist

class PipeLine:
    def create_spark_session(self):
        postgres_driver_path = "/Users/phanhoangnguyen/LearnNew/big_data/postgresql-42.6.0.jar"
        self.spark = (SparkSession.builder
                      .appName('Spark ETL Pipeline')
                      .master("local[*]")
                      .config("spark.jars", postgres_driver_path)
                      .config("spark.driver.host", "127.0.0.1")
                      .config("spark.hadoop.dfs.unmaskmode", "000")
                      .getOrCreate())

    def run_pipeline(self):
        # Use a breakpoint in the code line below to debug your script.
        print('Running Pipeline')

        ingest_process = ingest.Ingestion(spark=self.spark)
        df = ingest_process.ingest_data()
        df.show()

        transform_process = transform.Transformation(spark=self.spark)
        transform_df = transform_process.transform_data(df)
        transform_df.show()

        persist_process = persist.Persist(spark=self.spark)
        persist_process.persist_data(transform_df)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    pipeline = PipeLine()
    pipeline.create_spark_session()
    pipeline.run_pipeline()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
