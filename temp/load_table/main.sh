#!/bin/sh
# main.sh
# argument 1: database name


if [ $# -eq 0 ]
then
       echo 'Usage: main.sh DB_NAME'
       exit 1
fi

db=$1

mysql -e 'use '$db
if [ $? -eq 1 ]
then
echo 'Database '$db' does not exist.'
exit 1;
fi




python3 table_create.py NC absentee '../../local/NC/meta/mod_layout_absentee.txt' > tmp.sql
mysql $1 < tmp.sql
mysql $1 -e "LOAD DATA LOCAL INFILE '../../local/NC/data/absentee_20181106.csv' INTO TABLE absentee FIELDS ENCLOSED BY '\"' TERMINATED BY ',' ;"



