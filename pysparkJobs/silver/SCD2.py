from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField
from pyspark.sql.utils import AnalysisException

from pysparkJobs.metadata.Metadata import ctl_loading_field_name
from pysparkJobs.spark_utils.SQLHelper import gen_nvl_comparison


class SourceDesc:
    def __init__(self, surrogate_fields: List[StructField], source_key_fields: List[StructField],
                 business_key_fields: List[StructField], technical_fields: List[StructField]) -> None:
        self.surrogate_fields = surrogate_fields
        self.source_key_fields = source_key_fields
        self.business_key_fields = business_key_fields
        self.technical_fields = technical_fields
        self.schema = StructType(
            self.surrogate_fields + self.source_key_fields + self.business_key_fields + self.technical_fields)


class SCD2:
    def __init__(self, spark: SparkSession, ctl_loading: int, ctl_loading_date: str, hadoop_namenode: str,
                 source_desc: SourceDesc, s2t_mapping: List[List[str]],
                 target_table_name: str, source_table_name: str) -> None:
        self.spark = spark
        self.ctl_loading = ctl_loading
        self.ctl_loading_date = ctl_loading_date
        self.hadoop_namenode = hadoop_namenode
        self.table_dir = self.hadoop_namenode + "src/data/silver/" + target_table_name
        self.source_dir = self.hadoop_namenode + "src/data/bronze/" + source_table_name
        self.temp_dir = self.hadoop_namenode + "src/data/silver/temp/" + target_table_name
        self.table_name = target_table_name
        self.source_desc = source_desc
        self.s2t_mapping = s2t_mapping



    def get_source_df(self, incremental_ctl_loading: int) -> DataFrame:

        # Get Source Table with increment
        source_df = self.spark.read.orc(self.source_dir).filter(f"{ctl_loading_field_name}>{incremental_ctl_loading}")
        for l in self.s2t_mapping:
            source_df = source_df.withColumnRenamed(l[0], l[1])
        return source_df



    def upload_scd2_tmp_table(self, incremental_ctl_loading: int) -> None:
        # Get current table
        try:
            current_df = self.spark.read.parquet(self.table_dir)
        except AnalysisException:
            current_df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), self.source_desc.schema)
        #

        current_df.createOrReplaceTempView("current_df")

        source_df = self.get_source_df(incremental_ctl_loading)
        source_df.createOrReplaceTempView("source_increment")

        current_table_alias = "c"
        source_table_alias = "s"
        # Get new current records for dataframe
        new_records_sql = f"""
        SELECT {",     ".join([f"{current_table_alias}.{x.name}" for x in self.source_desc.surrogate_fields])},
               {",     ".join([f"{source_table_alias}.{x.name} " for x in self.source_desc.source_key_fields])},
               {",     ".join([f"{source_table_alias}.{x.name} " for x in self.source_desc.business_key_fields])},
               {self.ctl_loading} as ctl_loading,
               DATE('{self.ctl_loading_date}') AS effective_from,
               DATE('9999-12-31') AS effective_to,
               BOOLEAN(1) AS is_current
          FROM current_df {current_table_alias}
         INNER 
          JOIN source_increment {source_table_alias}
            ON {" AND ".join([f"{current_table_alias}.{x.name} = {source_table_alias}.{x.name}" for x in self.source_desc.source_key_fields])} 
           AND is_current = TRUE 
         WHERE {" OR ".join([gen_nvl_comparison(x, source_table_alias, current_table_alias) for x in self.source_desc.business_key_fields])}
        """

        df_new_records = self.spark.sql(new_records_sql)
        new_records_table_name = self.table_name + "_new_records_table_name"
        df_new_records.createOrReplaceTempView(new_records_table_name)

        # get previous records
        df_modified_recs = df_new_records.select([x.name for x in self.source_desc.surrogate_fields])
        if df_modified_recs.count() == 0:
            df_modified_recs = df_modified_recs\
                .union(self.spark.sql(
                    "SELECT" + ",     ".join([f" NULL AS {x.name}" for x in self.source_desc.surrogate_fields])))

        modified_table_name = self.table_name + "_modified_keys"
        df_modified_recs.createOrReplaceTempView(modified_table_name)

        # Expire previous current records
        new_hist_rec_sql = f"""
        SELECT {",     ".join([f"{current_table_alias}.{x.name}" for x in self.source_desc.surrogate_fields])},
               {",     ".join([f"{current_table_alias}.{x.name} " for x in self.source_desc.source_key_fields])},
               {",     ".join([f"{current_table_alias}.{x.name} " for x in self.source_desc.business_key_fields])},
               {self.ctl_loading} as ctl_loading, 
               {current_table_alias}.effective_from,
               DATE_SUB(DATE('{self.ctl_loading_date}'), 1) as effective_to,
               BOOLEAN(0) as is_current
          FROM current_df {current_table_alias}
         INNER 
          JOIN {modified_table_name} m 
            ON {" AND ".join([f"{current_table_alias}.{x.name} = m.{x.name}" for x in self.source_desc.surrogate_fields])}     
         WHERE {current_table_alias}.is_current = TRUE    
        """
        df_new_hist_recs = self.spark.sql(new_hist_rec_sql)
        new_hist_table_name = self.table_name + "_new_hist_recs"
        df_new_hist_recs.createOrReplaceTempView(new_hist_table_name)

        # Isolate unaffected records

        sql_unaffected_records = f"""
        SELECT {",     ".join([f"{current_table_alias}.{x.name}" for x in self.source_desc.surrogate_fields])},
               {",     ".join([f"{current_table_alias}.{x.name} " for x in self.source_desc.source_key_fields])},
               {",     ".join([f"{current_table_alias}.{x.name} " for x in self.source_desc.business_key_fields])},
               {current_table_alias}.ctl_loading, 
               {current_table_alias}.effective_from,
               {current_table_alias}.effective_to,
               {current_table_alias}.is_current
          FROM current_df {current_table_alias}
          LEFT 
          JOIN {modified_table_name} m 
            ON {" AND ".join([f"{current_table_alias}.{x.name} = m.{x.name}" for x in self.source_desc.surrogate_fields])}     
         WHERE m.{self.source_desc.surrogate_fields[0].name} IS NULL 
        """
        df_unaffected_records = self.spark.sql(sql_unaffected_records)
        df_unaffected_records.show()
        unaffected_records_table_name = self.table_name + "_unaffected_records"
        df_unaffected_records.createOrReplaceTempView(unaffected_records_table_name)

        # Create records for new records

        sql_new_source_key_fields = f"""
        SELECT {",     ".join([f"{source_table_alias}.{x.name} " for x in self.source_desc.source_key_fields])},
               {",     ".join([f"{source_table_alias}.{x.name} " for x in self.source_desc.business_key_fields])},
               {self.ctl_loading} as ctl_loading,
               DATE('{self.ctl_loading_date}') AS effective_from,
               DATE('9999-12-31') AS effective_to,
               BOOLEAN(1) AS is_current
          FROM current_df {current_table_alias}
         RIGHT 
          JOIN source_increment {source_table_alias}
            ON {" AND ".join([f"{current_table_alias}.{x.name} = {source_table_alias}.{x.name}" for x in self.source_desc.source_key_fields])}     
         WHERE {current_table_alias}.{self.source_desc.source_key_fields[0].name} IS NULL 
        """
        df_new_source_key_fields = self.spark.sql(sql_new_source_key_fields)
        new_source_keys_table_name = self.table_name + "_new_source_keys"
        df_new_source_key_fields.createOrReplaceTempView(new_source_keys_table_name)

        # Combine the datasets for new SCD2

        max_key = dict()
        for field in self.source_desc.surrogate_fields:
            tmp_max_key = \
                self.spark.sql(f"SELECT STRING(COALESCE(max({field.name}), 0)) FROM  current_df").collect()[0][0]
            max_key[field.name] = tmp_max_key

        sql_new_scd2 = f"""
        WITH a_cte AS 
        (
            SELECT {",     ".join([f"{x.name} " for x in self.source_desc.source_key_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.business_key_fields])},
                   ctl_loading,
                   effective_from,
                   effective_to,
                   is_current
              FROM {new_source_keys_table_name}

             UNION 
               ALL 

            SELECT {",     ".join([f"{x.name} " for x in self.source_desc.source_key_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.business_key_fields])},
                   ctl_loading,
                   effective_from,
                   effective_to,
                   is_current
              FROM {new_source_keys_table_name}  
        ),
        b_cte AS 
        (
            SELECT {",       ".join([f"ROW_NUMBER() OVER (ORDER BY effective_from) + BIGINT('{max_key[x.name]}') AS {x.name}" for x in self.source_desc.surrogate_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.source_key_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.business_key_fields])},
                   ctl_loading,
                   effective_from,
                   effective_to,
                   is_current
              FROM a_cte
        )
            SELECT {",       ".join([f"{x.name}" for x in self.source_desc.surrogate_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.source_key_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.business_key_fields])},
                   ctl_loading,
                   effective_from,
                   effective_to,
                   boolean(is_current) as is_current
              FROM b_cte

             UNION
               ALL 

            SELECT {",       ".join([f"{x.name}" for x in self.source_desc.surrogate_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.source_key_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.business_key_fields])},
                   ctl_loading,
                   effective_from,
                   effective_to,
                   boolean(is_current) as is_current
              FROM {unaffected_records_table_name}

             UNION
               ALL 

            SELECT {",       ".join([f"{x.name}" for x in self.source_desc.surrogate_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.source_key_fields])},
                   {",     ".join([f"{x.name} " for x in self.source_desc.business_key_fields])},
                   ctl_loading,
                   effective_from,
                   effective_to,
                   boolean(is_current) as is_current
              FROM {new_hist_table_name}
        """

        self.spark.sql(sql_new_scd2).write.mode("overwrite").partitionBy("is_current").parquet(self.temp_dir)

    def upload_scd2_final_table(self):
        df = self.spark.read.parquet(self.temp_dir).repartition(5)
        df.write.mode("overwrite") \
            .partitionBy("is_current") \
            .parquet(self.table_dir)
