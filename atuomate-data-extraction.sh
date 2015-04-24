#!/bin/bash

START_DATE=$(date -d "2015-01-31" +"%Y-%m-%d")

PATH_TO_HIVE_DATA_STORE="hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/WDPRO-CUSTANALYTICS-PROD/temparchive/personalization/hive"

PATH_TO_FINAL_DATA_STORE="hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/WDPRO-CUSTANALYTICS-PROD/archive/personalization/$DIR_MONTH"

HIVE_EXECUTABLE_PATH="/usr/bin/hive"

function copy_over_data_into_results_table()
{
        # This is the date passed in as the first arguement
        TARGET_DATE=$1

        # This will select the data from
        HIVE_QUERY= hive -e " set hive.cli.print.header=true; USE wdpro; INSERT OVERWRITE TABLE tmp_table SELECT hit_time_gmt
,date_time
,visid_high
,visid_low
,tnt
,user_agent
,referrer
,event_list
,page_event
,page_event_var1
,page_event_var2
,page_event_var3
,page_url
,pagename
,click_tag
FROM etl_omni_sec_raw
WHERE date_suite = '${TARGET_DATE}' AND  prop44 like 'wdgwdprowdw,%' ; "

        # Remove previous data
#       hdfs dfs -rm -f -R "${PATH_TO_HIVE_DATA_STORE}"

        #Create a directory in hdfs
        hdfs dfs -mkdir -p  hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/WDPRO-CUSTANALYTICS-PROD/archive/personalization/$DIR_MONTH

        # Recreate folder in readiness for incoming data
        hdfs dfs -mkdir -p "${PATH_TO_HIVE_DATA_STORE}"

        # Copy data from Source to Destination
        $HIVE_EXECUTABLE_PATH -e "${HIVE_QUERY}"

        mkdir -p  /home/WDPRO-CUSTANALYTICS-PROD/archive/personalization/$DIR_MONTH

        #Including the headers to the text file
        cat headers > /home/WDPRO-CUSTANALYTICS-PROD/archive/personalization/$DIR_MONTH/$TEXT_DATE.txt

        #Converting the .deflate file to .txt file
       hdfs dfs -text hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/WDPRO-CUSTANALYTICS-PROD/temparchive/personalization/hive/000000_0.deflate >> /home/WDPRO-CUSTANALYTICS-PROD/archive/personalization/$DIR_MONTH/$TEXT_DATE.txt

        #To upload the text file to HDFS path
        hdfs dfs -put /home/WDPRO-CUSTANALYTICS-PROD/archive/personalization/$DIR_MONTH/$TEXT_DATE.txt hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/WDPRO-CUSTANALYTICS-PROD/archive/personalization/$DIR_MONTH/$TEXT_DATE.txt

        #Delete the files from server
        rm -rf /home/WDPRO-CUSTANALYTICS-PROD/archive/personalization/$DIR_MONTH

        # Copy data to final destination
#       hdfs dfs -cp "${PATH_TO_HIVE_DATA_STORE}/000000_0.deflate"  "${PATH_TO_FINAL_DATA_STORE}/$TARGET_DATE.txt"
}



for ((INCREMENT=0; INCREMENT<=1; INCREMENT++ ))
do
   CURRENT_DATE=$(date -d "${START_DATE} + ${INCREMENT} days" +"%Y-%m-%d")
   TEXT_DATE=$(date -d "${START_DATE} + ${INCREMENT} days" +"%Y%m%d")
   DIR_MONTH=$(date -d "${START_DATE} + ${INCREMENT} days" +"%Y%m")
   echo "Copying data for current date = ${CURRENT_DATE}"

   copy_over_data_into_results_table $CURRENT_DATE
done
