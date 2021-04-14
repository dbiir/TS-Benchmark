#!/bin/bash
##计算导入时间
DB_TEST_MODE=$1 
DATA_GEN_HOME="$(cd "`dirname "$0"`"/.; pwd)"
echo $DATA_GEN_HOME
echo "calc time now..."
start=$(date +%s.%N); \
  if [ $DB_TEST_MODE -eq 1 ]; then 
  	influx -import -path=$DATA_GEN_HOME/influxdb.csv -precision=ms; \
	echo $DATA_GEN_HOME/influxdb.csv has been imported
  else
        timescaledb-parallel-copy --db-name ruc_test --table sensor \
	--file timescaledb.csv --workers 4 --copy-options "CSV";
  fi
  dur=$(echo "$(date +%s.%N) - $start" | bc); \
  printf "Execution time: %.6f seconds\n" $dur
