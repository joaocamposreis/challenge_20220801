main_folder="challenge"
local_prefix="/efs/home/ps616623/environment"
hdfs_prefix="/user/ps616623"

chmod 777 $local_prefix
dos2unix $local_prefix/*

sh $local_prefix/init.sh $local_prefix/$main_folder $hdfs_prefix/$main_folder

Copy trips.csv to "local" folder

Copy all python scripts and shell scripts to "scripts" folder

dos2unix $local_prefix/$main_folder/scripts/*

Execute DDL script test_trips.sql, to create table on Hive!

sh $local_prefix/$main_folder/scripts/workflow_ingestion.sh $local_prefix/$main_folder/ $hdfs_prefix/$main_folder/

sh $local_prefix/$main_folder/scripts/workflow_datamodel.sh $local_prefix/$main_folder/ $hdfs_prefix/$main_folder/
