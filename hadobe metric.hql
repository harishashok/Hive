USE da_tables;
CREATE EXTERNAL TABLE IF NOT EXISTS hadoop_adobe_qc(
date_suite STRING,line_of_business STRING,hadoop_visits DOUBLE,hadoop_visitors DOUBLE,hadoop_ticket_orders DOUBLE,hadoop_ticket_revenue DOUBLE,hadoop_resort_orders DOUBLE,hadoop_resort_revenue DOUBLE
)

PARTITIONED BY (date_partition STRING)
CLUSTERED BY (date_suite) INTO 1 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE hadoop_adobe_qc  PARTITION(date_partition)
SELECT t1.date_suite,t1.line_of_business,t1.hadoop_visits ,t1.hadoop_visitors ,t2.hadoop_ticket_orders ,t2.hadoop_ticket_revenue ,t2.hadoop_resort_orders ,t2.hadoop_resort_revenue,t2.date_partition
FROM
(
SELECT date_suite,line_of_business_entry AS line_of_business,
CAST(COUNT(DISTINCT visitor_id,visit_num)AS DOUBLE) hadoop_visits,
CAST (COUNT(DISTINCT visitor_id) AS DOUBLE) hadoop_visitors,
date_suite AS date_partition
FROM da_tables.da_main_wdw
WHERE date_partition='${yesterday}'
GROUP BY date_suite, line_of_business_entry
) as t1

INNER JOIN (

SELECT date_suite, line_of_business,
CAST(COUNT(distinct(CASE WHEN product_type="tix" THEN purchase_id END ))AS DOUBLE) hadoop_ticket_orders,
CAST(SUM(CASE WHEN product_type="tix" THEN revenue END) AS DOUBLE) hadoop_ticket_revenue,
CAST(COUNT(distinct(CASE WHEN product_type="res" THEN purchase_id END))AS DOUBLE) hadoop_resort_orders,
CAST(SUM(CASE WHEN product_type="res" THEN revenue END)AS DOUBLE) hadoop_resort_revenue,
date_suite AS date_partition
FROM da_tables.da_purchase_wdw
WHERE date_partition='${yesterday}'
GROUP BY date_suite, line_of_business
)AS t2 ON t1.line_of_business=t2.line_of_business
;
