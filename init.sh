#!/bin/bash

local_dir="$1" 
hdfs_dir="$2" 

echo "###### Creating Directory Local and HDFS ######"

#local challenge main folder
mkdir $local_dir
#open local challenge dir
cd $local_dir
#sink where the CSV will be saved!
mkdir $local_dir/local
#scripts folder
mkdir $local_dir/scripts
#json log according with Features
mkdir $local_dir/result
#pipelines log stored
mkdir $local_dir/logs
# hdfs main folder
hdfs dfs -mkdir $hdfs_dir
# sink where CSV will be copied from local
hdfs dfs -mkdir $hdfs_dir/lake
#json where result  will be stored
hdfs dfs -mkdir $hdfs_dir/result
hdfs dfs -mkdir $hdfs_dir/result/average
hdfs dfs -mkdir $hdfs_dir/result/similar
