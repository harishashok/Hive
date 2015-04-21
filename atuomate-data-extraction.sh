#!/bin/bash
 
START_DATE=$(date -d "2014-08-01" +"%Y-%m-%d")
 
PATH_TO_HIVE_DATA_STORE="hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/WDPRO-CUSTANALYTICS-PROD/path-to-hive-tables"
 
PATH_TO_FINAL_DATA_STORE="hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/WDPRO-CUSTANALYTICS-PROD/path-to-destination-tables-final"
 
HIVE_EXECUTABLE_PATH="/usr/bin/hive"
 
function copy_over_data_into_results_table()
{
	# This is the date passed in as the first arguement
	TARGET_DATE=$1
	
	# This will select the data from 
	HIVE_QUERY="INSERT OVERWRITE TABLE `wdpro.tmp_table` SELECT DISTINCT a,b,c FROM wdpro.original_table WHERE datesuite = '${TARGET_DATE}' AND colname LIKE adobemobile,% " 
	
	# Remove previous data
	hdfs dfs -rm -f -R "${PATH_TO_HIVE_DATA_STORE}"
	
	# Recreate folder in readiness for incoming data
	hdfs dfs -mkdir -p "${PATH_TO_HIVE_DATA_STORE}"
	
	# Copy data from Source to Destination
	$HIVE_EXECUTABLE_PATH -e "${HIVE_QUERY}"
	
	# Copy data to final destination
	hdfs dfs -cp "${PATH_TO_HIVE_DATA_STORE}/filename-part-00000" "${PATH_TO_FINAL_DATA_STORE}/$TARGET_DATE.txt"
}
 
for ((INCREMENT=1; INCREMENT<=240; INCREMENT++ ))
do
   CURRENT_DATE=$(date -d "${START_DATE} + ${INCREMENT} days" +"%Y-%m-%d")
   
   echo "Copying data for current date = ${CURRENT_DATE}"
   
   copy_over_data_into_results_table $CURRENT_DATE
done
