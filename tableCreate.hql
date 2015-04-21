# Creating a temporary table in Hive

USE WDPRO;

DROP TABLE IF EXISTS `tmp_table`;

CREATE EXTERNAL TABLE `tmp_table`(
`hit_time_gmt` int
,`date_time` string
,`visid_high` bigint
,`visid_low` bigint
,`tnt` string
,`user_agent` string
,`referrer` string
,`event_list` map<int,string>)
COMMENT 'Temporary table containing selected fields from etl_omni_sec_raw'
CLUSTERED BY (purchaseid) INTO 1 BUCKETS
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/WDPRO-CUSTANALYTICS-PROD/temparchive/personalization/hive/';
