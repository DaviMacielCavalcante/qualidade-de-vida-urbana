from pipe.generate_df import generate_df 
from pyspark.sql import SparkSession

def generate_parquet():
    pandas_df = generate_df()

    spark = SparkSession.builder.appName("parquet_generator").getOrCreate()

    spark_df = spark.createDataFrame(pandas_df)

    spark_df.show()

    spark_df.write.mode("append").partitionBy("year", "month", "day").parquet("../datalake/raw")