#!/bin/bash
. /var/dataservices/dataservices.properties
. /var/dataservices/dataservices.passwords.properties

ARGS=$(getopt -o sefh -l "table:,exportDir:,date_suite:" -n "$COMMAND" -- "$@")
ERROREXISTS=0

eval set -- "$ARGS";

while true ; do
case "$1" in
    --table) shift;table="$1";shift;;
    --exportDir) shift;exportDir="$1";shift;;
    --date_suite) shift;date_suite="$1";shift;;
    --) shift;break;;
  esac
done

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
outdir="${DIR}/sqoop/java"
mkdir -p $outdir

function printHelp {
    echo "Usage: $COMMAND [OPTION]"
    echo " --table MYSQL Table"
    echo " --exportDir HDFS path to export (/data/hive/wdpro.db/...)"
    echo " --date_suite"
}

function paramIsEmpty {
  if [ "$1" == "" ] ; then
ERROREXISTS=1;
  fi
}

paramIsEmpty "${table}" "--table"
paramIsEmpty "${exportDir}" "--exportDir"
paramIsEmpty "${date_suite}" "--date_suite"

# Print help message and exit
if [ "$ERROREXISTS" -ne "0" ] ; then
printHelp
  exit $ERROREXISTS
fi

database="bookingpropensity"


function export {
sqoop export -Dsqoop.export.statements.per.transaction=10000 \
             -Dsqoop.export.records.per.statement=100 \
             --num-mappers 20 \
             --connect "${db_systemcatalog_url}" \
             --username "${db_systemcatalog_username}" \
             --password "${db_systemcatalog_password}" \
             --table "${table}" \
	     --staging-table "${table}_staging" \
             --export-dir "${exportDir}" \
             --input-fields-terminated-by '\t' \
             --input-lines-terminated-by '\n' \
             --input-null-non-string ' ' \
             --input-null-string ' ' \
             --batch \
             --outdir $outdir \
             --clear-staging-table
}

function export_proc {
sqoop export -Dsqoop.export.statements.per.transaction=10000 \
             -Dsqoop.export.records.per.statement=1 \
             --num-mappers 1 \
             --connect ${db_systemcatalog_url}?noAccessToProcedureBodies=true \
             --username ${db_systemcatalog_username} \
             --password ${db_systemcatalog_password} \
             --call insert_aggregate \
             --export-dir ${exportDir} \
             --input-fields-terminated-by '\t' \
             --input-lines-terminated-by '\n' \
             --input-null-non-string ' ' \
             --input-null-string ' ' \
             --batch \
             --outdir $outdir 
}

function describe {
sqoop eval --connect "${db_systemcatalog_url}" \
           --username "${db_systemcatalog_username}" \
           --password "${db_systemcatalog_password}" \
           --query "select count(*) from ${table};"

sqoop eval --connect "${db_systemcatalog_url}" \
           --username "${db_systemcatalog_username}" \
           --password "${db_systemcatalog_password}" \
	   --query "describe ${table};"

sqoop eval --connect "${db_systemcatalog_url}" \
           --username "${db_systemcatalog_username}" \
           --password "${db_systemcatalog_password}" \
           --query "show create table ${table};"
}

function truncate {
sqoop eval --connect "${db_systemcatalog_url}" \
           --username "${db_systemcatalog_username}" \
           --password "${db_systemcatalog_password}" \
           --query "DELETE FROM ${table} WHERE date_suite='${date_suite}';"
}

function modify {
sqoop eval --connect "${db_systemcatalog_url}" \
           --username "${db_systemcatalog_username}" \
           --password "${db_systemcatalog_password}" \
           --query "alter table ${table} MODIFY mostfreqdomain VARCHAR(1024);"
}

function duplicate {
sqoop eval --connect "${db_systemcatalog_url}" \
           --username "${db_systemcatalog_username}" \
           --password "${db_systemcatalog_password}" \
           --query "CREATE TABLE ${table}_staging LIKE ${table};"
}

#duplicate
truncate
#describe
export_proc
#modify
