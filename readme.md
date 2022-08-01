# Data Engineering Challenge

## Assignment

<em>
Your task is to build an automatic process to ingest data on an on-demand basis. The data
represents trips taken by different vehicles, and include a city, a point of origin and a destination.
</em>.

Please create this VARIABLES on UNIX Environment:
```console
main_folder="challenge"
local_prefix="/environment"
hdfs_prefix="/user/xpto"

chmod 777 $local_prefix
dos2unix $local_prefix/*
```

```console
sh $local_prefix/init.sh $local_prefix/$main_folder $hdfs_prefix/$main_folder
```

Copy trips.csv to "local" folder

Copy all python scripts and shell scripts to "scripts" folder

Execute DDL script test_trips.sql, to create table on Hive!

```console
dos2unix $local_prefix/$main_folder/scripts/*

sh $local_prefix/$main_folder/scripts/workflow_ingestion.sh $local_prefix/$main_folder/ $hdfs_prefix/$main_folder/

sh $local_prefix/$main_folder/scripts/workflow_datamodel.sh $local_prefix/$main_folder/ $hdfs_prefix/$main_folder/

```
