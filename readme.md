# Data Engineering Challenge

## Assignment

<em>Your task is to build an automatic process to ingest data on an on-demand basis. The data
represents trips taken by different vehicles, and include a city, a point of origin and a destination.</em>

For this challenge, I used this tech/tools/frameworks:
  - Hadoop Cluster (using Yarn)
  - Pyspark using master yarn, with dynamic allocation, for in case of multiples files in ingestion phase, or large amount records in table transformation phase, spark will provide and adjusts the resources according application demand.
  - Hive table, partitioned by DT_PARTITION field.

### Environment Initiation

Please create this VARIABLES on UNIX Environment and run the commands below:
  - Remember to change user 123 in hdfs_prefix to a valid HDFS user according with Environment will be deployed.
  -  environment variables
```console
main_folder="challenge"
local_prefix="/environment"
hdfs_prefix="/user/123"
```
  -  permissions and file format command 
```console
chmod 777 $local_prefix
dos2unix $local_prefix/*
```

Run the init shell script to create folders to be used by pipeline, folder on Local Disk and HDFS:
```console
sh $local_prefix/init.sh $local_prefix/$main_folder $hdfs_prefix/$main_folder
```

Please copy the CSV File from [local](https://github.com/joaocamposreis/challenge_20220801/tree/main/local) to challenge/local folder.

Please copy the pytho all python files and shell files from [scripts](https://github.com/joaocamposreis/challenge_20220801/tree/main/scripts) challenge/scripts folder.

Please, execute DDL create table script [test_trips.sql](https://github.com/joaocamposreis/challenge_20220801/blob/main/sql/test_trips.sql), to create table in Hive.

Apply chmod in scripts folder:
```console
dos2unix $local_prefix/$main_folder/scripts/*
```
### Ingestion

Run ingestion shell script to ingest all CSV files to HIVE Parquet table partioned, important observations below:

   - Every execution will ingest all CSV Files available on local.
   - Every execution will read the Hive table, merge current state (Files already stored) with new state (New files) and will repartition and store again.
   - The table is partitioned by Month and Year Extract from field "datetime", in format MMyyyy.
   - The log of ingestion will be available in challenge/logs/ingestion_"datetime".out 
    
```console
sh $local_prefix/$main_folder/scripts/workflow_ingestion.sh $local_prefix/$main_folder/ $hdfs_prefix/$main_folder/
```

### Data Model Transformation

Run data model shell script to process and create result according Features rules, important observations below:

   - Every execution will analyze all records available on table.
   - Every execution will analyze and create the Features and show the result on console.
   - The result will be stored in HDFS, json format, and after copied from HDFS to local in folder, after that will be show on console application:
      - Feature 1 "Trips with similar origin, destination, and time of day should be grouped together", 
        will be stored in challenge/result/similar.
      - Feature 2 "Develop a way to obtain the weekly average number of trips for an area, defined by abounding box (given by coordinates) or by a region"
        will be stored in challenge/result/average.
   - The log of ingestion will be available in challenge/logs/datamodel_"datetime".out 
    
```console
sh $local_prefix/$main_folder/scripts/workflow_datamodel.sh $local_prefix/$main_folder/ $hdfs_prefix/$main_folder/
```
