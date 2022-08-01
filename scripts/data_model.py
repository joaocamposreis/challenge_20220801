from datetime import datetime
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.window import Window
import sys


spark = SparkSession.builder.\
        master('yarn').\
        appName('app_datamodel').\
        enableHiveSupport(). \
        getOrCreate()


datalake_result=sys.argv[1] 
table=sys.argv[2] 

df = spark.read.table(table)

# Calculates the great-circle distance (in km) between two GPS points p1 and p2 in km
def distance(lat_p1, lon_p1, lat_p2, lon_p2):
    return acos(
        sin(toRadians(lat_p1)) * sin(toRadians(lat_p2)) + 
        cos(toRadians(lat_p1)) * cos(toRadians(lat_p2)) * 
            cos(toRadians(lon_p1) - toRadians(lon_p2))
    ) * lit(6371.0)

# Data Cleaning origin_coord/destination_coord
df_cleaning1 = df \
    .withColumn("tmp1",regexp_replace("origin_coord","POINT ","")) \
    .withColumn("tmp2",regexp_replace("tmp1"," ",",")) \
    .withColumn("lat",split(col("tmp2"),",").getItem(1)) \
    .withColumn("long",split(col("tmp2"),",").getItem(0)) \
    .withColumn("lat1",regexp_replace("lat","\)","")) \
    .withColumn("long1",regexp_replace("long","\(","")) \
    .drop("tmp1") \
    .drop("tmp2") \
    .drop("lat") \
    .drop("long") \
    .drop("origin_coord") \

df_cleaning2 = df_cleaning1 \
    .withColumn("tmp1",regexp_replace("destination_coord","POINT ","")) \
    .withColumn("tmp2",regexp_replace("tmp1"," ",",")) \
    .withColumn("lat",split(col("tmp2"),",").getItem(1)) \
    .withColumn("long",split(col("tmp2"),",").getItem(0)) \
    .withColumn("lat2",regexp_replace("lat","\)","")) \
    .withColumn("long2",regexp_replace("long","\(","")) \
    .drop("tmp1") \
    .drop("tmp2") \
    .drop("lat") \
    .drop("long") \
    .drop("destination_coord") \

df_distance = df_cleaning2.withColumn("distance",distance(col("lat1"),col("long1"),col("lat2"),col("long2"))).withColumn("distance",col("distance").cast("decimal(18,3)")).cache()

# Feature 1
df_similar_origin = df_distance.selectExpr("region","round(lat1) as lat","round(long1) as long","date_format(datetime,'HH') as time_of_day") \
                        .groupBy("region","lat","long","time_of_day").agg(count("*").alias("quantity")).orderBy(col("lat").desc(),col("time_of_day").desc())

df_similar_destin = df_distance.selectExpr("region","round(lat2) as lat","round(long2) as long","date_format(datetime,'HH') as time_of_day") \
                        .groupBy("region","lat","long","time_of_day").agg(count("*").alias("quantity")).orderBy(col("lat").desc(),col("time_of_day").desc())

df_feat1 = df_similar_origin.withColumn("dataset",lit("origin")).union(df_similar_destin.withColumn("dataset",lit("destination")))

df_feat1.coalesce(1).write.mode("overwrite").json(datalake_result + "/similar")

#Feature 2
w =  Window.partitionBy(col("region")).orderBy(col("region").desc())

df_feat2_temp = df_distance \
            .withColumn("week",weekofyear(col("datetime"))) \
            .groupBy("region","week") \
            .agg(count("*").alias("quantity_week")) \
            .orderBy(col("region").asc()) \
            
df_feat2 = df_feat2_temp.withColumn("avg_week",avg(col("quantity_week")).over(w))

df_feat2.coalesce(1).write.mode("overwrite").json(datalake_result + "/average")



