import sys

from pyspark.sql import SparkSession

from pysparkJobs.metadata.SparkArgParser import SparkArgParser


class WeeklyBusinessAggregate:
    def __init__(self, spark: SparkSession, ctl_loading: int, ctl_loading_date: str, hadoop_namenode: str):
        self.spark = spark
        self.ctl_loading = ctl_loading
        self.ctl_loading_date = ctl_loading_date
        self.hadoop_namenode = hadoop_namenode
        self.target_table_name = "weekly_business_aggregate"

    def upload_table(self):
        fact_checkin = spark.read.parquet("hdfs://namenode:9000/src/data/silver/fact_checkins")
        fact_checkin.createOrReplaceTempView("fact_checkins")
        dim_businesses = spark.read.parquet("hdfs://namenode:9000/src/data/silver/dim_businesses")
        dim_businesses.createOrReplaceTempView("dim_businesses")
        fact_reviews = spark.read.parquet("hdfs://namenode:9000/src/data/silver/fact_reviews")
        fact_reviews.createOrReplaceTempView("fact_reviews")
        fact_tips= spark.read.parquet("hdfs://namenode:9000/src/data/silver/fact_tips")
        fact_tips.createOrReplaceTempView("fact_tips")


        # Depending on the team background some teams prefer to use SQL or to use DataFrame sequentially approach
        # Here probably second one can be used for simplicity to analyze errors
        sql_str = """
        WITH max_interval as
        (
        SELECT min(min_date) as min_date,
               max(max_date) as max_date
          FROM (
            SELECT min(tips_date) as min_date,
                   max(tips_date) as max_date 
              FROM fact_tips 
             WHERE ctl_loading_date = (select max(ctl_loading_date) from fact_tips)
        
             UNION
               ALL 
               
            SELECT min(checkin_date) as min_date,
                   max(checkin_date) as max_date 
              FROM fact_checkins
             WHERE ctl_loading_date = (select max(ctl_loading_date) from fact_checkins) 
             
             
             UNION
               ALL 
               
            SELECT min(review_date) as min_date,
                   max(review_date) as max_date 
              FROM fact_reviews
             WHERE ctl_loading_date = (select max(ctl_loading_date) from fact_reviews) 
               )
        ), week_dates AS 
        (
        SELECT EXPLODE (date) AS week_date
          FROM
               (
            SELECT sequence(trunc(to_date(min_date), 'week'), trunc(to_date(max_date), 'week'), interval 1 week) as date
              FROM max_interval
                ) d
        ), week_intervals as 
        (
        SELECT week_date start_week_date, date_add(LEAD(week_date, 1, DATE('5999-12-31')) over (ORDER BY week_date), -1) end_week_date
          FROM week_dates
        ), business_ids as 
        (
        SELECT business_id 
          FROM dim_businesses
         WHERE is_current = True  
        ), review_measures as 
        (
        SELECT business_id,
               start_week_date,
               end_week_date,
               weekly_useful_cnt weekly_review_useful_cnt,
               sum(weekly_useful_cnt) OVER (PARTITION BY business_id ORDER BY start_week_date) as aggregated_review_useful_cnt,
               weekly_funny_cnt weekly_review_funny_cnt,
               sum(weekly_funny_cnt) OVER (PARTITION BY business_id ORDER BY start_week_date) as aggregated_review_funny_cnt,
               weekly_cool_cnt weekly_review_cool_cnt,
               sum(weekly_cool_cnt) OVER (PARTITION BY business_id ORDER BY start_week_date) as aggregated_review_cool_cnt,
               CASE WHEN weekly_stars_cnt !=0 
                      THEN CAST(weekly_stars_sum * 1.0 / weekly_stars_cnt AS decimal (4, 2)) 
                    ELSE NULL 
               END AS weekly_review_stars_avg,
               CASE WHEN SUM(weekly_stars_cnt) OVER (PARTITION BY business_id ORDER BY start_week_date) !=0 
                      THEN CAST(SUM(weekly_stars_sum) OVER (PARTITION BY business_id ORDER BY start_week_date) 
                              * 1.0 
                              / SUM(weekly_stars_cnt) OVER (PARTITION BY business_id ORDER BY start_week_date) AS decimal (4, 2)) 
                    ELSE NULL 
               END AS aggregated_review_stars_avg
          FROM 
               (
            SELECT b.business_id,
                   w.start_week_date, 
                   w.end_week_date,
                   sum(coalesce(review_useful_cnt, 0)) as weekly_useful_cnt,
                   sum(coalesce(review_funny_cnt, 0)) as weekly_funny_cnt,
                   sum(coalesce(review_cool_cnt, 0)) as weekly_cool_cnt,
                   sum(coalesce(review_stars, 0)) as weekly_stars_sum,
                   count(review_stars) as weekly_stars_cnt
              FROM week_intervals w
             CROSS 
              JOIN business_ids b
              LEFT 
              JOIN (
                 SELECT * 
                   FROM fact_reviews 
                  WHERE ctl_loading_date in (select max(ctl_loading_date) FROM fact_reviews)
                   ) r 
                ON b.business_id = r.business_id 
               AND r.review_date >= w.start_week_date
               AND r.review_date <= w.end_week_date
             GROUP  
                BY w.start_week_date, 
                   w.end_week_date,
                   b.business_id 
               )
        ), checkin_measures as  
        (
        SELECT business_id,
               start_week_date, 
               end_week_date,
               weekly_checkin_appearence_cnt,
               SUM (weekly_checkin_appearence_cnt) OVER (PARTITION BY business_id ORDER BY start_week_date) as aggregated_checkin_appearence_cnt
          FROM (
            SELECT c.business_id,
                   w.start_week_date, 
                   w.end_week_date,
                   SUM(COALESCE(c.cnt_appearence, 0)) as weekly_checkin_appearence_cnt
              FROM week_intervals w
             CROSS 
              JOIN business_ids b
              LEFT 
              JOIN (
                 SELECT * 
                   FROM fact_checkins
                  WHERE ctl_loading_date in (select max(ctl_loading_date) FROM fact_checkins)
                   ) c 
                ON b.business_id = c.business_id 
               AND c.checkin_date >= w.start_week_date
               AND c.checkin_date <= w.end_week_date
             GROUP  
                BY w.start_week_date, 
                   w.end_week_date,
                   c.business_id 
                )
        ), tips_measures as 
        (
        SELECT business_id,
               start_week_date, 
               end_week_date,
               weekly_tips_cnt,
               SUM (weekly_tips_cnt) OVER (PARTITION BY business_id ORDER BY start_week_date) AS aggregated_tips_cnt,
               weekly_complimnets_cnt,
               SUM (weekly_complimnets_cnt) OVER (PARTITION BY business_id ORDER BY start_week_date) AS aggregated_complimnets_cnt         
          FROM (
            SELECT t.business_id,
                   w.start_week_date, 
                   w.end_week_date,
                   SUM(COALESCE(t.tips_cnt, 0)) as weekly_tips_cnt,
                   SUM(COALESCE(t.tips_sum_of_compliments, 0)) as weekly_complimnets_cnt
              FROM week_intervals w
             CROSS 
              JOIN business_ids b
              LEFT 
              JOIN (
                 SELECT * 
                   FROM fact_tips
                  WHERE ctl_loading_date in (select max(ctl_loading_date) FROM fact_tips)
                   ) t
                ON b.business_id = t.business_id 
               AND t.tips_date >= w.start_week_date
               AND t.tips_date <= w.end_week_date
             GROUP  
                BY w.start_week_date, 
                   w.end_week_date,
                   t.business_id 
                )
        )
        SELECT r.business_id,
               b.business_name, 
               r.end_week_date,
               b.business_address,
               b.business_city,
               b.business_postal_code,
               b.business_stars as final_amount_business_stars,
               b.business_review_count as final_amount_business_review_count,
               r.weekly_review_useful_cnt,
               r.aggregated_review_useful_cnt,
               r.weekly_review_funny_cnt,
               r.aggregated_review_funny_cnt,
               r.weekly_review_cool_cnt,
               r.aggregated_review_cool_cnt,
               r.weekly_review_stars_avg,
               r.aggregated_review_stars_avg,
               c.weekly_checkin_appearence_cnt,
               c.aggregated_checkin_appearence_cnt,
               t.weekly_tips_cnt,
               t.aggregated_tips_cnt,
               t.weekly_complimnets_cnt,
               t.aggregated_complimnets_cnt      
               
          FROM review_measures r
          LEFT 
          JOIN checkin_measures c 
            ON r.business_id = c.business_id
           AND r.start_week_date = c.start_week_date
          LEFT 
          JOIN tips_measures t 
            ON r.business_id = t.business_id
           AND r.start_week_date = t.start_week_date
          LEFT 
          JOIN dim_businesses b 
            ON r.business_id = b.business_id 
        """

        self.spark.sql(sql_str).write.mode("overwrite")\
            .format("parquet")\
            .save(f"{self.hadoop_namenode}src/data/gold/{self.target_table_name}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Businesses").getOrCreate()
    parser = SparkArgParser()
    argums = parser.parse_args(sys.argv[1:])
    ctl_loading = argums.ctl_loading
    ctl_loading_date = argums.ctl_loading_date

    wkl = WeeklyBusinessAggregate(spark, ctl_loading, ctl_loading_date, "hdfs://namenode:9000/")
    wkl.upload_table()

