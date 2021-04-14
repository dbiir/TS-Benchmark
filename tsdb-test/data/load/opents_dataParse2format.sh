#!/usr/bin/env bash
# 计算当前目录
BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
echo $BENCHMARK_HOME
# 启动程序
#!/bin/bash
# BuildAndRun.sh
# 编译并运行java代码
 
file="OpentsdbUtils.java"
 
# 根据文件名来获取生成的class名称
class=$(echo $file | awk -F '.' '{print $1}')
echo "class= $class"
echo "building!"
echo "------------------------------------"
 
# 编译
javac $file
 
if [ $? -eq 0 ]; then
    echo "build success, prepare to run，formatting data into style which can be imported."
    echo "------------------------------------"
 
    # 运行
    java $class $BENCHMARK_HOME
    if [ $? -eq 0 ]; then
        echo "------------------------------------"
        echo "success!!!"
    else
        echo "------------------------------------"
        echo "running error!!!"
    fi
else
    echo "------------------------------------"
    echo "build error!"
fi
 
# 防止影响后面的测试，运行完毕之后，可以将生成的class文件删除
if [ -f $class.class ]; then
    rm -rf $class.class
fi
