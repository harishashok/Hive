#!/bin/sh
. /var/dataservices/dataservices.properties

#checks if yesterdays partition is in the table
function checkPartitions {

    yesterday=$(date -d "Yesterday" +"%Y-%m-%d")

    table_name="da_main_wdw"

    partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    while [ ${yesterday} -ne ${partitions} ]
    do
        echo "${table_name}: missing partitions "
        sleep 900
        partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    done

    table_name="da_purchase_wdw"
    partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    while [ ${yesterday} -ne ${partitions} ]
    do
        echo "${table_name}: missing partitions"
        sleep 900
        partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    done

    hive -f ${basepath}/updatenew.hql -hiveconf yesterday=${yesterday}
    ${/home/WDPRO-CUSTANALYTICS-DEV/services/monitoring-data-dataservices}/sqoop_export.sh --table sc_hadoop_adobe_qc --date_suite $(date -d "Yesterday" +"%Y-%m-%d") --exportDir /data/hive/wdpro.db/da_tables.db/hadoop_adobe_qc/date_partition=$(date -d "Yesterday" +"%Y-%m-%d")
}
checkPartitions
[WDPRO-CUSTANALYTICS-DEV@QN7PRIDABI01 ]$ ls
hadobemain.sh  pig_1424287847917.log  updatenew.hql
[WDPRO-CUSTANALYTICS-DEV@QN7PRIDABI01 ]$ vi updatenew.hql
[WDPRO-CUSTANALYTICS-DEV@QN7PRIDABI01 ]$ cat updatenew.hql
USE da_tables;
CREATE EXTERNAL TABLE IF NOT EXISTS hadoop_adobe_qc (
date_suite STRING, lob STRING, source STRING, metric STRING, value DOUBLE
)
PARTITIONED BY (date_partition STRING)
CLUSTERED BY (date_suite) INTO 1 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE hadoop_adobe_qc  PARTITION(date_partition)
SELECT date_suite, lob, source, metric, value, date_partition
FROM
(
SELECT date_suite,lob, 'da_main_wdw' AS source, 'visitors' AS metric, CAST(COUNT(DISTINCT visitor_id) AS DOUBLE) AS value, date_suite AS date_partition
FROM da_tables.da_main_wdw
WHERE date_suite='${yesterday}'
GROUP BY date_suite, lob))

UNION ALL

SELECT date_suite,lob, 'da_main_wdw' AS source, 'visits' AS metric, CAST(COUNT(DISTINCT visitor_id,visit_num)AS DOUBLE) AS value, date_suite AS date_partition
FROM da_tables.da_main_wdw
WHERE date_suite='${yesterday}'
GROUP BY date_suite, lob))

UNION ALL

SELECT date_suite, lob, 'da_purchase_wdw' AS source, 'ticket_orders' AS metric, CAST(COUNT(distinct(CASE WHEN product_type="tix" THEN purchase_id END ))AS DOUBLE) AS value ,'date_suite' AS date_partition
FROM da_tables.da_purchase_wdw
WHERE date_suite='${yesterday}'
GROUP BY date_suite, lob))

UNION ALL

SELECT date_suite, lob, 'da_purchase_wdw' AS source, 'ticket_revenue' AS metric, CAST(SUM(CASE(WHEN product_type="tix" THEN revenue END)) AS DOUBLE)  AS value,'date_suite' AS date_partition
FROM da_tables.da_purchase_wdw
WHERE date_suite='${yesterday}'
GROUP BY date_suite, lob))

UNION ALL

SELECT date_suite, lob, 'da_purchase_wdw' AS source, 'resort_orders' AS metric, CAST(COUNT(distinct(CASE WHEN product_type="res" THEN purchase_id END))AS DOUBLE) AS value ,'date_suite' AS date_partition
FROM da_tables.da_purchase_wdw
WHERE date_suite='${yesterday}'
GROUP BY date_suite, lob))

UNION ALL

SELECT date_suite, lob, 'da_purchase_wdw' AS source, 'resort_revenue' AS metric, CAST(SUM(CASE(WHEN product_type="res" THEN revenue END))AS DOUBLE)  AS value,'date_suite' AS date_partition
FROM da_tables.da_purchase_wdw
WHERE date_suite='${yesterday}'
GROUP BY date_suite, lob))


) tmp
WHERE value>0
;
