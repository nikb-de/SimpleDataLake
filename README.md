# SimpleDataLake


# Project docker architecture 
Project consists of 9 docker images in [docker-compose](docker-compose.yml):
- namenode - image for Apache Hadoop namenode 
- datanode-1 - image for Apache Hadoop datanode-1
- datanode-2 - image for Apache Hadoop datanode-2
- spark-master - image for master node of spark standalone cluster 
- spark-worker-1 - image for worker node of spark standalone cluster
- spark-worker-2 - image for worker node of spark standalone cluster
- spark-worker-3 - image for worker node of spark standalone cluster
- ftpd_server - image ([Dockerfile](sourceData/Dockerfile)) for FTP server which will simulate source system
- pyspark-etl - image ([Dockerfile](pysparkJobs/Dockerfile)) for PySpark Jobs for Data Lake

To start docker containers

```bash 
docker-compose up --build
```

Before docker start you should first upload source data!

# Project Data Lake Infrastructure
![Data Lake layers](docs/pics/Data%20Lake%20Architecture.png)

There are 4 layers
##  FTP Source. 
This layer consists of 5 json files. These files expected to be filled before data processing and docker starting. Check [README](sourceData/README.md) for information

## Bronze layer 
This layer consists of 5 ORC "tables" and represents raw storage. [Source code](pysparkJobs/bronze/BronzeEtl.py) for it's filling. To run data pipeline job do:
Each table partitioned by "ctl_loading" field, which is a technical identifier for data load.
```bash 
docker exec -it pyspark-etl /bin/bash
```

Inside docker container 
```bash 
cd /app && sh run_bronze_etl.sh
```

## Silver layer
This layer consists of 5 Parquet "tables". Each table 
Two dimensions as SCD2:
- [dim_users](pysparkJobs/silver/Users.py)
- [dim_businesses](pysparkJobs/silver/Businesses.py)

Three Snapshot Facts:
- [fact_tips](pysparkJobs/silver/Tips.py)
- [fact_reviews](pysparkJobs/silver/Reviews.py)
- [fact_checkibs](pysparkJobs/silver/CheckIns.py)

Silver layer represents Star schema, where you have 2 dimension historical tables and 3 snapshot fact table. Usually we will store here historical data.

To run data pipeline for whole layer population do:

```bash 
docker exec -it pyspark-etl /bin/bash
```

Inside docker container 
```bash 
cd /app && sh run_silver_etl.sh
```


## Gold Layer 
This layer consists of final aggregate PARQUET "table" [weekly_business_aggregate](pysparkJobs/gold/WeeklyBusinessAggregate.py)

Gold layer here is a Data Mart part. So we have fully denormalized aggregated structure which can be queried by BI tools. 

To run data pipeline for whole layer population do:

```bash 
docker exec -it pyspark-etl /bin/bash
```

Inside docker container 
```bash 
cd /app && sh run_gold_etl.sh
```


## Potential improvements 
- Add scheduler and orchesrator, e.g. Airflow
- Add metadata management system, e.g. Postgresql + Self-Written Service 
- Make more generous algorithm for SCD2, for examples, when by increment comes several changes per one business_id or user_id
- Make some synthetic generator for incremental loading test
- Add tests for code. Due to time limit it has not been done yet 

## Materials Used in Task 
- [Dockerfile](https://github.com/jakobhviid/DataScienceCourseSDU/blob/master/pysparkExampleImage/Dockerfile) - docker-compose by which I made my docker-compose 
- [Docker image](https://github.com/big-data-europe) - Big Data Europe team project, who provided docker images for Apache Spark and Apache Hadoop 
- [Blogpost](https://towardsdatascience.com/processing-a-slowly-changing-dimension-type-2-using-pyspark-in-aws-9f5013a36902) - SCD2 Algorithm for Spark, which I have generalised a bit
