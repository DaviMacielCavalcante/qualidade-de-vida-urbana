from pipe import generate_df as gen
from pyspark.sql import SparkSession

pandas_df = gen.generate_df()

spark = SparkSession.builder.appName("parquet_generator").getOrCreate()

spark_df = spark.createDataFrame(pandas_df)

spark_df.show()

spark_df.write.mode("append").partitionBy("year", "month", "day").parquet("./datalake/raw")






