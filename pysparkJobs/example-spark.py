from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from pyspark import SparkFiles

from pysparkJobs.bronze.source.reader.FtpReader import FtpReader
from pysparkJobs.bronze.source.reader.SourceDesc import SourceDesc
from pysparkJobs.metadata.Metadata import Metadata

spark = SparkSession.builder.appName("Test").master("local").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")
ftp_address = "172.200.0.30"
user_name = "username"
password = "mypass"

log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)
log.warn("Hello World!")

file_name_list = ["yelp_academic_dataset_business.json", "yelp_academic_dataset_checkin.json",
                  "yelp_academic_dataset_review.json", "yelp_academic_dataset_tip.json",
                  "yelp_academic_dataset_user.json"]

ftp_reader = FtpReader(spark, ftp_address, user_name, password)
# dataSource = "ftp://username:mypass@172.200.0.30/yelp_academic_dataset_business.json"
# data_source = ftp_connection_string + "yelp_academic_dataset_business.json"
# print(spark.sparkContext.getConf().getAll())
# spark.sparkContext.addFile(dataSource)
df = ftp_reader.read_source_entity_to_df(SourceDesc("json", "yelp_academic_dataset_business.json",
                                                    {'primitivesAsString': 'true', 'allowSingleQuotes': 'true'}))
df_data_path = SparkFiles.get("yelp_academic_dataset_business.json")
df.show()
# [('spark.master', 'local'), ('spark.driver.port', '34349'), ('spark.rdd.compress', 'True'), ('spark.app.startTime', '1617049277557'), ('spark.serializer.objectStreamReset', '100'), ('spark.app.name', 'BronzeEtlRun'), ('spark.driver.host', '24c376f6b494'), ('spark.submit.pyFiles', ''), ('spark.executor.id', 'driver'), ('spark.submit.deployMode', 'client'), ('spark.sql.warehouse.dir', 'file:/opt/project/pysparkJobs/bronze/spark-warehouse'), ('spark.app.id', 'local-1617049278281'), ('spark.ui.showConsoleProgress', 'true')]
# [('spark.master', 'local'), ('spark.driver.port', '43687'), ('spark.rdd.compress', 'True'), ('spark.app.name', 'Test'), ('spark.serializer.objectStreamReset', '100'), ('spark.app.startTime', '1617049356369'), ('spark.submit.pyFiles', ''), ('spark.executor.id', 'driver'), ('spark.submit.deployMode', 'client'), ('spark.sql.warehouse.dir', 'file:/opt/project/pysparkJobs/spark-warehouse'), ('spark.ui.showConsoleProgress', 'true'), ('spark.app.id', 'local-1617049357061'), ('spark.driver.host', 'fcb0eace8514')]


# print(SparkFiles.getRootDirectory())
print(df_data_path)
spark.stop()
# ftp_reader = FtpReader(spark, ftp_adress, user_name, password, log)
# metadata = Metadata("1", "2020-01-01")

#
# for file in file_name_list:
#     df = ftp_reader.readFileToDF(file, "json", {'primitivesAsString':'true', 'allowSingleQuotes':'true'})
#     df_with_meta = Metadata.add_metadata_fields_to_df(df)


# final_path = "hdfs://namenode:9000/" + "src/data/yelp_academic_dataset_business"
# # print("file://" + df_data_path)
# # with open(df_data_path) as testFile:
# #     for line in testFile:
# #         print(line)
# rdd = sc.textFile(df_data_path)
# # print(rdd.collect())
# df = spark.read.json(rdd, primitivesAsString=True, allowSingleQuotes=True, multiLine=True)
# df.write.orc(final_path)
# spark.sql
