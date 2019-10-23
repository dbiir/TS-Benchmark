###timescaledb测试方法

- 运行
python generate_timescale.py
生成timescaledb.csv

- 运行
sh csv_dataGen.sh 1


###timescaledb测试方法
- 运行
python generate_timescale.py
生成timescaledb.csv

- 运行
切换至项目根目录
cd ../../
sh init_timescaledb.sh
sh csv_dataGen.sh 2


###Druid测试方法
1. 在当前目录pwd
2. 复制路径
3. 将目录下的tsbm-druid_index.json复制到druid安装目录下的quickstart目录下，修改“ioConfig":{"baseDir":"###"},将###替換成经过处理的json文件目录
4. 将tsbm-druid_index.json 复制至druid安装目录下quickstart文件夹下
5. 启动druid(ubuntu上：nohup bin/supervise -c conf/supervise/quickstart.conf > quickstart.log &)
6. 在druid安装目录下，运行./bin/post-index-task --file ./quickstart/tsbm-druid_index.json
