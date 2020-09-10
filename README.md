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

#### params description

The configuration of TSDBs is shown as follows:

- InfluxDB. We enlarge the default values of some important parameters of the TSM engine for better performance of the system. For example, the parameter wal-fsync- delay is set as ”0s”, the parameter cache-max-memory- size is set to 1,048,576,000 bytes, and the parameter cache-snapshot-memory-size is enlarged to “100M” and so on. Maximum memory size is sufficient. The parameter max-values-per-tag is set as 0 to allow an unlimited number of tag values.

- TimescaleDB. The parameter shared-buffers is set as 8GB, maintenance-work-mem is set as 2GB, checkpoint- completion-target is set as 0.7, min_wal size is set as 1GB, and max wal size is 2GB. Parameters of Post- greSQL is set based on PgTune .

- Druid. The parameter Roll-up is set as true, and Granu- larity is set as hour. For local batch import, the parameter maxRowsPerSegment is set as 10M, maxRowsInMemory is set as 20M, and maxTotalRows is set as 100M. 

- OpenTSDB. The parameter tsd-http-request-enable- chunked is enabled, tsd-http-request-max-chunk is set as 32KB, tsd-core-auto-create-metrics is set as true, and the parameter tsd-storage-enable-compaction is set to be false to improve the write performance.

具体步骤请查看[时序评测工具使用手册](./documents/时序评测工具使用手册.pdf)
