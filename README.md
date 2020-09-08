# Tsdb-benchmark

#### 描述
时序数据库评测工具V2

#### 基本流程

完成评测的一般步骤为：

1. 数据生成
```cd data_generation```
- 先运行DCGAN.py文件，训练DCGAN
	```python DCGAN.py```
- 接着运行encoder_dc.py文件，训练encoder
	```python encoder_dc.py```
- 最后执行测试
	```python test_dc.py```
2. 数据导入
   由于各个数据库对于数据导入支持模式不同，我们定义了一些数据导入相关的工具在tsdb-test/data/load目录下
3. 性能测试
4. 结果查看

具体步骤请查看[时序评测工具使用手册](./documents/时序评测工具使用手册.pdf)
