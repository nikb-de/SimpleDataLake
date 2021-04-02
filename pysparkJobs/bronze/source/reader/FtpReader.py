from logging import Logger

from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any

from pysparkJobs.bronze.source.reader.SourceDesc import SourceDesc
from pysparkJobs.bronze.source.reader.SourceReader import SourceReader


class FtpReader(SourceReader):
    def __init__(self, spark: SparkSession, ftp_address: str, user_name: str, password: str):
        self.spark = spark
        self.ftp_address = ftp_address
        self.user_name = user_name
        self.password = password
        self.ftp_connection_string = f"ftp://{self.user_name}:{self.password}@{self.ftp_address}/"

    def read_source_entity_to_df(self, source_desc: SourceDesc) -> DataFrame:
        file_name = source_desc.entity_name + "." + source_desc.source_type
        data_source = self.ftp_connection_string + file_name
        # For spark-alone cluster i have to use dirty hack to run spark in local, because addFile method
        # creates hadoop file, so for standalone spark it doesn't redistribute across nodes
        print(data_source)
        # if  ("ftp://username:mypass@172.200.0.30/yelp_academic_dataset_business.json" == data_source):
        #     print(True)
        # else:
        #     print(False)
        # print(self.spark.sparkContext.getConf().getAll())
        self.spark.sparkContext.addFile(data_source)

        df_data_path = SparkFiles.get(file_name)
        df = self.spark.read.format(source_desc.source_type).options(**source_desc.spark_additional_props).load(
            df_data_path)
        return df
