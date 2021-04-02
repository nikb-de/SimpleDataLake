import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import DateType

from pysparkJobs.metadata.Metadata import ctl_loading_date_field_name, ctl_loading_field_name
from pysparkJobs.metadata.SparkArgParser import SparkArgParser


class Tips:
    def __init__(self, spark: SparkSession, ctl_loading: int, ctl_loading_date: str, hadoop_namenode: str):
        self.spark = spark
        self.ctl_loading = ctl_loading
        self.ctl_loading_date = ctl_loading_date
        self.hadoop_namenode = hadoop_namenode
        self.target_table_name = "fact_tips"
        self.source_table_name = "yelp_academic_dataset_tip"


    def get_source(self) -> DataFrame:
        df = self.spark.read.orc(f"{self.hadoop_namenode}src/data/bronze/{self.source_table_name}")
        df = df.withColumn("date", col("date").cast(DateType()))

        df.createOrReplaceTempView("source_data")

        business_df = spark.read.parquet(f"{self.hadoop_namenode}src/data/silver/dim_businesses")
        business_df.createOrReplaceTempView("dim_businesses")

        business_df = spark.read.parquet(f"{self.hadoop_namenode}src/data/silver/dim_users")
        business_df.createOrReplaceTempView("dim_users")

        sql_str = f"""
        SELECT coalesce(d.business_id, -1) as business_id,
               coalesce(u.user_id, -1) as user_id,
               date as tips_date, 
               count(*) as tips_cnt,
               sum(compliment_count) as tips_sum_of_compliments,  
               DATE('{self.ctl_loading_date}') as {ctl_loading_date_field_name},
               {self.ctl_loading} as {ctl_loading_field_name}
          FROM source_data s 
          LEFT 
          JOIN dim_businesses d
            ON s.business_id = d.business_nk
           AND DATE('{self.ctl_loading_date}') >= d.effective_from 
           AND DATE('{self.ctl_loading_date}') <= d.effective_from 
          LEFT
          JOIN dim_users u
            ON s.user_id = u.user_nk
           AND DATE('{self.ctl_loading_date}') >= d.effective_from 
           AND DATE('{self.ctl_loading_date}') <= d.effective_from 
         GROUP 
            BY date, 
               coalesce(d.business_id, -1),
               coalesce(u.user_id, -1) 
        """

        return spark.sql(sql_str)

    def upload_to_table(self, df: DataFrame) -> None:

        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        df.write.mode("overwrite")\
            .format("parquet").partitionBy(ctl_loading_date_field_name)\
            .save(f"{self.hadoop_namenode}src/data/silver/{self.target_table_name}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tips").getOrCreate()
    parser = SparkArgParser()
    argums = parser.parse_args(sys.argv[1:])
    ctl_loading = argums.ctl_loading
    ctl_loading_date = argums.ctl_loading_date

    check_in = Tips(spark, ctl_loading, ctl_loading_date, "hdfs://namenode:9000/")
    check_in.upload_to_table(check_in.get_source())