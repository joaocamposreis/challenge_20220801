
#!/bin/bash

local_dir="$1" 
hdfs_dir="$2" 
datetime=`date +%Y%m%d%H%M%S`
echo "###### Ingestion Worflow Start ######"

#copying all trips to datalake
echo "Transfer CSV files from local to HDFS.."
hdfs dfs -put -f $local_dir/local/trips*.csv $hdfs_dir/lake/

#start workflow
echo "Pyspark ingestion have started.. "
nohup spark-submit --master yarn --conf spark.dynamicAllocation.enabled=true $local_dir/scripts/ingestion.py $hdfs_dir/lake $hdfs_dir/result prd_product_orderentry_domesticmarketbr.test_trips > $local_dir/logs/ingestion_$datetime.out 2>&1
echo "Pyspark ingestion have finished, please check status on Log folder"

echo "###### Ingestion Worflow End ######"
