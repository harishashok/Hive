### This query will export the results of a hive query into a single text file###

set mapred.reduce.tasks=1;
set hive.exec.compress.output=false;
set hive.cli.print.header=true; -- 

USE database_name;

SELECT col_name1, col_name2, col_name3, col_name10 
FROM table_name 
WHERE date_suite='2015-04-21' AND name44 LIKE 'wdwadobewdw,%' ; 


#SHELL COMMAND:
#this commmand will run the hive query and it will export the results into a text file (default tab delimited)
hive -f 'exportHive.q' > /home/user/monthYear/month/20140421.txt

#This command will update the destination file with recent changes
hive -f 'exportHive.q' >> /home/user/monthYear/month/20140421.txt
