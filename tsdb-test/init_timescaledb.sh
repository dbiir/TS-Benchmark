#!/usr/bin/env bash
# 计算当前目录
BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
echo $BENCHMARK_HOME
#编译
mvn clean package -Dmaven.test.skip=true
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
MAIN_CLASS="cn.edu.ruc.TimescaledbUtils"

echo $CLASSPATH
exec "$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS"
exit $?
