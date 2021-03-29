from typing import List

from pyspark import SparkFiles
from pyspark.sql import DataFrame, SparkSession

from pysparkJobs.bronze.source.reader.FtpReader import FtpReader
from pysparkJobs.bronze.source.reader.SourceDesc import SourceDesc
from pysparkJobs.bronze.source.reader.SourceReader import SourceReader
from pysparkJobs.metadata.Metadata import Metadata, ctl_loading_field_name


class BronzeEtl:
    def __init__(self, spark: SparkSession, ctl_loading: int, ctl_loading_date: str, hadoop_namenode: str):
        self.ctl_loading = ctl_loading
        self.ctl_loading_date = ctl_loading_date
        self.namenode = hadoop_namenode
        self.spark = spark
        # self.spark = SparkSession.builder.appName("BronzeEtlRun").master("local").getOrCreate()
        self.metadata = Metadata(ctl_loading, ctl_loading_date)

    def upload_to_bronze_layer(self, file_reader: FtpReader, source_entities_list: List[SourceDesc]) -> None:
        for source_entity in source_entities_list:
            df = file_reader.read_source_entity_to_df(source_entity)
            df_with_meta = self.metadata.add_metadata_fields_to_df(df)
            final_path = self.namenode + "src/data/" + source_entity.entity_name
            df_with_meta.write \
                .partitionBy(ctl_loading_field_name) \
                .option("orc.compress", "snappy") \
                .mode("overwrite").orc(final_path)

    def upload_ftp_yelp_source(self, ftp_reader: FtpReader):
        # spark = SparkSession.builder.appName("BronzeEtlRun").master("local").getOrCreate()
        # sc = spark.sparkContext
        # sc.setLogLevel("WARN")
        # log4jLogger = sc._jvm.org.apache.log4j
        # log = log4jLogger.LogManager.getLogger(__name__)
        # log.warn("Hello World!")
        # ctl_loading = 1
        # ctl_loading_name = "2010-01-01 00:00:00"
        # hadoop_namenode = "hdfs://namenode:9000/"
        # bronze_etl = BronzeEtl(spark, ctl_loading, ctl_loading_name, hadoop_namenode)
        # ftp_address = "172.200.0.30"
        # user_name = "username"
        # password = "mypass"
        # file_reader = FtpReader(self.spark, ftp_address, user_name, password)
        basic_source_type = "json"
        basic_spark_props = {'primitivesAsString': 'true', 'allowSingleQuotes': 'true'}
        file_names_list = ["yelp_academic_dataset_business.json", "yelp_academic_dataset_checkin.json",
                           "yelp_academic_dataset_review.json", "yelp_academic_dataset_tip.json",
                           "yelp_academic_dataset_user.json"]
        # df = file_reader.read_source_entity_to_df(SourceDesc("json", "yelp_academic_dataset_business.json",
        #                                                      {'primitivesAsString': 'true', 'allowSingleQuotes': 'true'}))
        # df_data_path = SparkFiles.get("yelp_academic_dataset_business.json")
        # df.show()
        source_entities_list = [SourceDesc(basic_source_type, x, basic_spark_props) for x in file_names_list]
        self.upload_to_bronze_layer(ftp_reader, source_entities_list)
