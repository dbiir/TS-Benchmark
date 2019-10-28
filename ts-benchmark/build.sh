#!/usr/bin/env bash
# 定位至当前目录
BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
echo $BENCHMARK_HOME
#编译
mvn clean package install -Dmaven.test.skip=true
echo "基础依赖编译完成"
