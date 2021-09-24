#!/usr/bin/env bash
# pay attention to the params below and set refer to the notes
# 计算当前目录
BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
echo $BENCHMARK_HOME
#编译
#mvn clean package -Dmaven.test.skip=true
# 参数路径
PARAM_PATH=${BENCHMARK_HOME}"/"param.properties
# 启动程序
CLASSPATH=""
for f in ${BENCHMARK_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi
MAIN_CLASS="TSDBTest"

DATA_PATH="${BENCHMARK_HOME}"

rm -rf "${DATA_PATH}"/farm
rm -rf "${DATA_PATH}"/device
##测试数据库选择
# 1:influxdb ;2:timescaledb ;3:iotdb ;4 opentsdb;5 druid;6 GaussDB(for Influx);7 tdengine;8 AliHiTSDB; 9 AliLindorm; 10 AliInfluxDB
DB_CODE=5
##测试项选择
# 0: generate,1:i,w,r ,2 w,r
TEST_METHOD=0

exec "$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS" "$DB_CODE" "${TEST_METHOD}" "${DATA_PATH}"
exit $?
