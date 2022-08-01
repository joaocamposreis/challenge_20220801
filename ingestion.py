from datetime import datetime
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession
from pyspark import SparkContext
import sys

spark = SparkSession.builder.\
        master('yarn').\
        appName('app_ingestion').\
        enableHiveSupport(). \
        getOrCreate()

spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

datalake_csv=sys.argv[1] 
datalake_result=sys.argv[2] 
table=sys.argv[3] 


#Reading new files
df_stage = spark.read.option("header", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(datalake_csv)

#Reading full table
spark.catalog.refreshTable(table)

df_current = spark.read.table(table)

#Adding partition
df_stage_partition = df_stage.withColumn("DT_PARTITION",date_format(col("datetime"), 'MMyyyy').cast("String"))

#Union current snapshot and new files 
df_new_image = df_current.union(df_stage_partition)

#Overwrite new image
df_new_image.write.mode("overwrite").insertInto(table)

#Creating Metrics
df_output = df_new_image.agg(count("*").alias("records_updated")).withColumn("last_ingestion",date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))

#Save log in json
df_output.write.mode("append").json(datalake_result)


