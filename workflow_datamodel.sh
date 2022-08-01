
#!/bin/bash

local_dir="$1" 
hdfs_dir="$2" 
datetime=`date +%Y%m%d%H%M%S`
echo "###### Data Model Workflow Start ######"

#start workflow
echo "Pyspark Data Model have started.. "
nohup spark-submit --master yarn --conf spark.dynamicAllocation.enabled=true $local_dir/scripts/data_model.py $hdfs_dir/result prd_product_orderentry_domesticmarketbr.test_trips > $local_dir/logs/datamodel_$datetime.out 2>&1

#getting result from HDFS to local feature 1
echo "Getting result from HDFS and saving at locala about feature 1.."
hdfs dfs -getmerge $hdfs_dir/result/similar/*.json $local_dir/result/similar/challenge_feature1_$datetime.json

#getting result from HDFS to local feature 2
echo "Getting result from HDFS and saving at locala about feature 2.."
hdfs dfs -getmerge $hdfs_dir/result/average/*.json $local_dir/result/average/challenge_feature2_$datetime.json

#showing result about feature 1
echo "##################################### Feature 1 result #######################################"
cat $local_dir/result/similar/challenge_feature1_$datetime.json
echo "##################################### Feature 1 result #######################################"
echo "."
echo "."
echo "."
echo "."
#showing result about feature 2
echo "####################### Feature 2 result ########################"
cat $local_dir/result/average/challenge_feature2_$datetime.json
echo "####################### Feature 2 result ########################"
echo "###### Data Model Workflow End ######"
