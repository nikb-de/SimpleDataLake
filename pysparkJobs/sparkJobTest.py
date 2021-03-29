from pyspark import SparkFiles
from pyspark.sql import SparkSession

from pysparkJobs.bronze.BronzeEtl import BronzeEtl
from pysparkJobs.bronze.source.reader.FtpReader import FtpReader
from pysparkJobs.bronze.source.reader.SourceDesc import SourceDesc

spark = SparkSession.builder.appName("Test").master("local").getOrCreate()
# if __name__ == '__main__':
#     # spark = SparkSession.builder.appName("BronzeEtlRun").master("local").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")
ctl_loading = 1
ctl_loading_name = "2010-01-01 00:00:00"
hadoop_namenode = "hdfs://namenode:9000/"
ftp_address = "172.200.0.30"
user_name = "username"
password = "mypass"
file_reader = FtpReader(spark, ftp_address, user_name, password)
bronze_etl = BronzeEtl(spark, ctl_loading, ctl_loading_name, hadoop_namenode)
bronze_etl.upload_ftp_yelp_source(file_reader)
# df = file_reader.read_source_entity_to_df(SourceDesc("json", "yelp_academic_dataset_business.json",
#                                                     {'primitivesAsString': 'true', 'allowSingleQuotes': 'true'}))
# df_data_path = SparkFiles.get("yelp_academic_dataset_business.json")


spark.stop()