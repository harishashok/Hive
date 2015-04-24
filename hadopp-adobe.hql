
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
