--Please change database if have you use a different!!
use default;
DROP TABLE IF EXISTS test_trips;
CREATE TABLE test_trips
(
     REGION STRING
    ,ORIGIN_COORD STRING
    ,DESTINATION_COORD STRING
    ,`DATETIME` STRING
    ,DATASOURCE STRING
)
PARTITIONED BY(DT_PARTITION STRING)
STORED AS PARQUET;
