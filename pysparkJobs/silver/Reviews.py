import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

from pysparkJobs.metadata.Metadata import ctl_loading_date_field_name, ctl_loading_field_name
from pysparkJobs.metadata.SparkArgParser import SparkArgParser


class Reviews:
    def __init__(self, spark: SparkSession, ctl_loading: int, ctl_loading_date: str, hadoop_namenode: str):
        self.spark = spark
        self.ctl_loading = ctl_loading
        self.ctl_loading_date = ctl_loading_date
        self.hadoop_namenode = hadoop_namenode
        self.target_table_name = "fact_reviews"
        self.source_table_name = "yelp_academic_dataset_review"

    def get_source(self) -> DataFrame:
        df = self.spark.read.orc(f"{self.hadoop_namenode}src/data/bronze/{self.source_table_name}")
        # df.show(100, False)
        df = df.withColumn("date", col("date").cast(DateType()))

        df.createOrReplaceTempView("source_data")

        business_df = spark.read.parquet(f"{self.hadoop_namenode}src/data/silver/dim_business")
        business_df.createOrReplaceTempView("dim_business")

        business_df = spark.read.parquet(f"{self.hadoop_namenode}src/data/silver/dim_users")
        business_df.createOrReplaceTempView("dim_users")

        sql_str = f"""
            SELECT coalesce(d.business_id, -1) as business_id,
                   coalesce(u.user_id, -1) as user_id,
                   review_id as review_nk, 
                   date as review_date, 
                   text as review_text_desc,
                   stars as review_stars,  
                   useful as review_useful_cnt, 
                   funny as review_funny_cnt,
                   cool as review_cool_cnt,
                   DATE('{self.ctl_loading_date}') as {ctl_loading_date_field_name},
                   {self.ctl_loading} as {ctl_loading_field_name}
              FROM source_data s 
              LEFT 
              JOIN dim_business d
                ON s.business_id = d.business_nk
               AND DATE('{self.ctl_loading_date}') >= d.effective_from 
               AND DATE('{self.ctl_loading_date}') <= d.effective_from 
              LEFT
              JOIN dim_users u
                ON s.user_id = u.user_nk
               AND DATE('{self.ctl_loading_date}') >= d.effective_from 
               AND DATE('{self.ctl_loading_date}') <= d.effective_from 
            """

        return spark.sql(sql_str)

    def upload_to_table(self, df: DataFrame) -> None:
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.write.mode("overwrite") \
            .format("parquet").partitionBy(ctl_loading_date_field_name) \
            .save(f"{self.hadoop_namenode}src/data/silver/{self.target_table_name}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Reviews").getOrCreate()
    parser = SparkArgParser()
    argums = parser.parse_args(sys.argv[1:])
    ctl_loading = argums.ctl_loading
    ctl_loading_date = argums.ctl_loading_date

    check_in = Reviews(spark, ctl_loading, ctl_loading_date, "hdfs://namenode:9000/")
    check_in.upload_to_table(check_in.get_source())