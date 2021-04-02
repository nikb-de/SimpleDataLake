import sys
from typing import List

from pyspark.sql import DataFrame, SparkSession
from argparse import ArgumentParser
from pysparkJobs.bronze.source.reader.FtpReader import FtpReader
from pysparkJobs.bronze.source.reader.SourceDesc import SourceDesc
from pysparkJobs.metadata.Metadata import Metadata, ctl_loading_field_name
from threading import Thread
from multiprocessing import Pool

from pysparkJobs.metadata.SparkArgParser import SparkArgParser


class BronzeEtl:
    def __init__(self, spark: SparkSession, ctl_loading: int, ctl_loading_date: str, hadoop_namenode: str):
        self.ctl_loading = ctl_loading
        self.ctl_loading_date = ctl_loading_date
        self.namenode = hadoop_namenode
        self.spark = spark
        self.metadata = Metadata(ctl_loading, ctl_loading_date)

    def upload_to_bronze_layer(self, file_reader: FtpReader, source_entities_list: List[SourceDesc]) -> None:
        def etl_process(source_entity: SourceDesc):
            df = file_reader.read_source_entity_to_df(source_entity)
            df_with_meta = self.metadata.add_metadata_fields_to_df(df)
            final_path = self.namenode + "src/data/bronze/" + source_entity.entity_name
            # i have to hardcode repartition due to some internal errors via spark local
            df_with_meta \
                .repartition(10) \
                .write \
                .partitionBy(ctl_loading_field_name) \
                .option("orc.compress", "snappy") \
                .mode("overwrite").orc(final_path)

        for src in source_entities_list:
            etl_process(src)
        # TODO: run this code in parallel
        # threads = [Thread(target=thread_worker, args=(src,)) for src in source_entities_list]
        # for t in threads:
        #     t.start()
        #
        # for t in threads:
        #     t.join()

            # df = file_reader.read_source_entity_to_df(source_entity)
            # df_with_meta = self.metadata.add_metadata_fields_to_df(df)
            # final_path = self.namenode + "src/data/bronze/" + source_entity.entity_name
            # df_with_meta.write \
            #     .partitionBy(ctl_loading_field_name) \
            #     .option("orc.compress", "snappy") \
            #     .mode("overwrite").orc(final_path)

    def upload_ftp_yelp_source(self, ftp_reader: FtpReader) -> None:

        basic_source_type = "json"
        basic_spark_props = {'allowSingleQuotes': 'true'}
        # TODO: it is better to move to some config file probably for easiness to change
        file_names_list = ["yelp_academic_dataset_business", "yelp_academic_dataset_checkin",
                           "yelp_academic_dataset_review", "yelp_academic_dataset_tip",
                           "yelp_academic_dataset_user"]
        source_entities_list = [SourceDesc(basic_source_type, x, basic_spark_props) for x in file_names_list]
        self.upload_to_bronze_layer(ftp_reader, source_entities_list)

if __name__ == "__main__":
    parser = SparkArgParser()
    argums = parser.parse_args(sys.argv[1:])
    ctl_loading = argums.ctl_loading
    ctl_loading_date = argums.ctl_loading_date
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .getOrCreate()
    # if __name__ == '__main__':
    #     # spark = SparkSession.builder.appName("BronzeEtlRun").master("local").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    hadoop_namenode = "hdfs://namenode:9000/"
    ftp_address = "172.200.0.30"
    user_name = "username"
    password = "mypass"
    file_reader = FtpReader(spark, ftp_address, user_name, password)
    bronze_etl = BronzeEtl(spark, ctl_loading, ctl_loading_date, hadoop_namenode)
    bronze_etl.upload_ftp_yelp_source(file_reader)