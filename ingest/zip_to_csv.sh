#! /bin/bash 

YEAR=2015 
for month in `seq -w 1 1`; do
   unzip $YEAR$month.zip
   mv *ONTIME.csv $YEAR$month.csv
   rm $YEAR$month.zip
done