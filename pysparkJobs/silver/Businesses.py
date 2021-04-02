import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, IntegerType, DateType, FloatType, BooleanType, LongType

from pysparkJobs.metadata.Metadata import ctl_loading_field_name
from pysparkJobs.metadata.SparkArgParser import SparkArgParser
from pysparkJobs.silver.SCD2 import SCD2, SourceDesc


class Businesses(SCD2):

    def __init__(self, spark: SparkSession, ctl_loading: int, ctl_loading_date: str, hadoop_namenode: str):

        self.spark = spark
        self.ctl_loading = ctl_loading
        self.ctl_loading_date = ctl_loading_date
        self.hadoop_namenode = hadoop_namenode

        self.source_key_fields = [StructField("business_nk", StringType(), True)]
        self.business_key_fields = [
            StructField("business_name", StringType(), False),
            StructField("business_address", StringType(), False),
            StructField("business_city", StringType(), False),
            StructField("business_state", StringType(), False),
            StructField("business_postal_code", StringType(), False),
            StructField("business_latitude", FloatType(), False),
            StructField("business_longitude", FloatType(), False),
            StructField("business_stars", FloatType(), False),
            StructField("business_is_open", IntegerType(), False),
            StructField("business_review_count", StringType(), False)

         ]

        self.technical_fields = [StructField(ctl_loading_field_name, IntegerType(), False),
                                 StructField("effective_from", DateType(), False),
                                 StructField("effective_to", DateType(), False),
                                 StructField("is_current", BooleanType(), False)]

        self.surrogate_fields = [StructField("business_id", LongType(), True)]

        self.source_desc = SourceDesc(self.surrogate_fields, self.source_key_fields, self.business_key_fields,
                                      self.technical_fields)

        self.s2t_mapping = [
            ["business_id", "business_nk"],
            ["address", "business_address"],
            ["name", "business_name"],
            ["city", "business_city"],
            ["state", "business_state"],
            ["postal_code", "business_postal_code"],
            ["latitude", "business_latitude"],
            ["longitude", "business_longitude"],
            ["stars", "business_stars"],
            ["is_open", "business_is_open"],
            ["review_count", "business_review_count"]
        ]
        self.target_table_name = "dim_businesses"
        self.source_table_name = "yelp_academic_dataset_business"

        super().__init__(spark, self.ctl_loading, self.ctl_loading_date, self.hadoop_namenode, self.source_desc, self.s2t_mapping,
                         self.target_table_name, self.source_table_name)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Businesses").getOrCreate()
    parser = SparkArgParser()
    argums = parser.parse_args(sys.argv[1:])
    ctl_loading = argums.ctl_loading
    ctl_loading_date = argums.ctl_loading_date
    users = Businesses(spark, ctl_loading, ctl_loading_date, "hdfs://namenode:9000/")
    users.upload_scd2_tmp_table(0)
    users.upload_scd2_final_table()
