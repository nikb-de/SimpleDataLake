from pyspark import SparkFiles
from pyspark.sql import SparkSession

from pysparkJobs.bronze.BronzeEtl import BronzeEtl
from pysparkJobs.bronze.source.reader.FtpReader import FtpReader
from pysparkJobs.bronze.source.reader.SourceDesc import SourceDesc

spark = SparkSession.builder\
    .appName("Test")\
    .master("local[*]")\
    .getOrCreate()
# if __name__ == '__main__':
#     # spark = SparkSession.builder.appName("BronzeEtlRun").master("local").getOrCreate()

spark.sql("set spark.sql.shuffle.partitions=5")

df = spark.read.orc("hdfs://namenode:9000/src/data/bronze/yelp_academic_dataset_business")
df.printSchema()
df.select(df["attributes.BusinessParking"]).show(100, False)
# spark.read.parquet("hdfs://namenode:9000/src/data/silver/users").show(20, False)
# spark.read.parquet("hdfs://namenode:9000/src/data/silver/business").show(20, False)
spark.read.parquet("hdfs://namenode:9000/src/data/silver/fact_tips").show(20, False)
spark.read.parquet("hdfs://namenode:9000/src/data/silver/fact_checkins").show(20, False)
spark.read.parquet("hdfs://namenode:9000/src/data/silver/dim_businesses").show(20, False)
spark.read.parquet("hdfs://namenode:9000/src/data/silver/fact_reviews").show(20, False)
spark.read.parquet("hdfs://namenode:9000/src/data/silver/dim_users").show(20, False)
spark.read.parquet("hdfs://namenode:9000/src/data/gold/weekly_business_aggregate").show(20, False)
