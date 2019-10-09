#!/bin/sh
# main.sh
# argument 1: database name


if [ $# -eq 0 ]
then
       echo 'Usage: main.sh DB_NAME'
       exit 1
fi

$db=$1

mysql -e 'use '$db
if [ $? -eq 1 ]
then
echo 'Database '$db' does not exist.'
exit 1;
fi




python3 table_create.py '../local/NC/meta/layout_absentee.txt > tmp.sql
mysql $1 < tmp.sql


