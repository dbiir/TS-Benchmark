# Load Data Into DataBase Helper

### generate data for influxdb

``python generate_influx.py``
to generate influx.csv

then use param 1 run ``sh csv_dataGen.sh 1``



### generate data for timescaledb

run ``python generate_timescale.py``
to generate timescaledb.csv

- 
cd to the root dir of the tsdb-test
``cd ../../``
``sh init_timescaledb.sh``
``sh csv_dataGen.sh 2``


### generate data for Druid

1. pwd current dir [path]
2. copy the path
3. copy the tsbm-druid_index.json to [druid set up dir]/quickstart, modify the “ioConfig":{"baseDir":"###"}, replace the ### to the processed json file dir
4. start druid(example in ubuntu：``nohup bin/supervise -c conf/supervise/quickstart.conf > quickstart.log &``)
5. in druid set up dir, run ``./bin/post-index-task --file ./quickstart/tsbm-druid_index.json``
