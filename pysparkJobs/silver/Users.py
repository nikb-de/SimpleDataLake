import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, IntegerType, DateType, FloatType, BooleanType, LongType

from pysparkJobs.metadata.Metadata import ctl_loading_field_name
from pysparkJobs.metadata.SparkArgParser import SparkArgParser
from pysparkJobs.silver.SCD2 import SCD2, SourceDesc


class Users(SCD2):

    def __init__(self, spark: SparkSession, ctl_loading: int, ctl_loading_date: str, hadoop_namenode: str):
        self.spark = spark
        self.ctl_loading = ctl_loading
        self.ctl_loading_date = ctl_loading_date
        self.hadoop_namenode = hadoop_namenode

        self.source_key_fields = [StructField("user_nk", StringType(), True)]
        self.business_key_fields = [
            StructField("user_name", StringType(), False),
            StructField("user_review_count", IntegerType(), False),
            StructField("user_yelping_since", DateType(), False),
            StructField("user_useful_votes", IntegerType(), False),
            StructField("user_funny_votes", IntegerType(), False),
            StructField("user_cool_votes", IntegerType(), False),
            StructField("user_number_of_fans", IntegerType(), False),
            StructField("user_average_stars", FloatType(), False),
            StructField("user_received_compliment_hot", IntegerType(), False),
            StructField("user_received_compliment_more", IntegerType(), False),
            StructField("user_received_compliment_profile", IntegerType(), False),
            StructField("user_received_compliment_cute", IntegerType(), False),
            StructField("user_received_compliment_list", IntegerType(), False),
            StructField("user_received_compliment_note", IntegerType(), False),
            StructField("user_received_compliment_plain", IntegerType(), False),
            StructField("user_received_compliment_cool", IntegerType(), False),
            StructField("user_received_compliment_funny", IntegerType(), False),
            StructField("user_received_compliment_writer", IntegerType(), False),
            StructField("user_received_compliment_photos", IntegerType(), False)]

        self.technical_fields = [StructField(ctl_loading_field_name, IntegerType(), False),
                                 StructField("effective_from", DateType(), False),
                                 StructField("effective_to", DateType(), False),
                                 StructField("is_current", BooleanType(), False)]

        self.surrogate_fields = [StructField("user_id", LongType(), True)]

        self.source_desc = SourceDesc(self.surrogate_fields, self.source_key_fields, self.business_key_fields,
                                      self.technical_fields)

        self.s2t_mapping = [
            ["user_id", "user_nk"],
            ["name", "user_name"],
            ["review_count", "user_review_count"],
            ["yelping_since", "user_yelping_since"],
            ["useful", "user_useful_votes"],
            ["funny", "user_funny_votes"],
            ["cool", "user_cool_votes"],
            ["fans", "user_number_of_fans"],
            ["average_stars", "user_average_stars"],
            ["compliment_hot", "user_received_compliment_hot"],
            ["compliment_more", "user_received_compliment_more"],
            ["compliment_profile", "user_received_compliment_profile"],
            ["compliment_cute", "user_received_compliment_cute"],
            ["compliment_list", "user_received_compliment_list"],
            ["compliment_note", "user_received_compliment_note"],
            ["compliment_plain", "user_received_compliment_plain"],
            ["compliment_cool", "user_received_compliment_cool"],
            ["compliment_funny", "user_received_compliment_funny"],
            ["compliment_writer", "user_received_compliment_writer"],
            ["compliment_photos", "user_received_compliment_photos"]
        ]

        self.target_table_name = "dim_users"
        self.source_table_name = "yelp_academic_dataset_user"

        super().__init__(spark, self.ctl_loading, self.ctl_loading_date, self.hadoop_namenode, self.source_desc, self.s2t_mapping,
                         self.target_table_name, self.source_table_name)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Users").getOrCreate()
    parser = SparkArgParser()
    argums = parser.parse_args(sys.argv[1:])
    ctl_loading = argums.ctl_loading
    ctl_loading_date = argums.ctl_loading_date

    users = Users(spark, ctl_loading, ctl_loading_date, "hdfs://namenode:9000/")
    users.upload_scd2_tmp_table(0)
    users.upload_scd2_final_table()

