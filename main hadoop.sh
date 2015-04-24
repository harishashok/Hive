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

     table_name="da_main_dcl"

    partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    while [ ${yesterday} -ne ${partitions} ]
    do
        echo "${table_name}: missing partitions"
        sleep 900
        partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    done

    table_name="da_main_dlr"

    partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    while [ ${yesterday} -ne ${partitions} ]
    do
        echo "${table_name}: missing partitions"
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

    table_name="da_purchase_dcl"
    partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    while [ ${yesterday} -ne ${partitions} ]
    do
        echo "${table_name}: missing partitions"
        sleep 900
        partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    done

    table_name="da_purchase_dlr"
    partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    while [ ${yesterday} -ne ${partitions} ]
    do
        echo "${table_name}: missing partitions"
        sleep 900
        partitions=$( hive -e "USE da_tables; SELECT COUNT(DISTINCT date_suite) FROM ${table_name} WHERE date_suite='${yesterday}';")
    done

    hive -f ${monitoring_data_base}hadoop_adobe/update.hql -hiveconf yesterday=${yesterday}
    ${monitoring_data_base}/sqoop_export.sh --table sc_hadoop_adobe_qc --date_suite $(date -d "Yesterday" +"%Y-%m-%d") --exportDir /data/hive/wdpro.db/da_tables.db/sc_hadoop_adobe_qc/date_partition=$(date -d "Yesterday" +"%Y-%m-%d")
}
checkPartitions
