# TS-Benchmark: A Benchmark for Time Series Databases

#### Description

This project is the source code of TS-Benchmark. Related work has been published in ICDE 2021.
bibTex:
```
@inproceedings{DBLP:conf/icde/HaoQCLSTZD21,
  author    = {Yuanzhe Hao and
               Xiongpai Qin and
               Yueguo Chen and
               Yaru Li and
               Xiaoguang Sun and
               Yu Tao and
               Xiao Zhang and
               Xiaoyong Du},
  title     = {TS-Benchmark: {A} Benchmark for Time Series Databases},
  booktitle = {37th {IEEE} International Conference on Data Engineering, {ICDE} 2021,
               Chania, Greece, April 19-22, 2021},
  pages     = {588--599},
  publisher = {{IEEE}},
  year      = {2021},
  url       = {https://doi.org/10.1109/ICDE51399.2021.00057},
  doi       = {10.1109/ICDE51399.2021.00057},
  timestamp = {Mon, 28 Jun 2021 10:16:44 +0200},
  biburl    = {https://dblp.org/rec/conf/icde/HaoQCLSTZD21.bib},
  bibsource = {dblp computer science bibliography, https://dblp.org}
}
```

#### Start

The general steps to complete the test are:

1. Data generation 
``cd data_generation``

2. Train DCGAN model
``python DCGAN.py``
Run the ``encoder_dc.py`` file to train the encoder, ``python encoder_dc.py``
Finally execute the test ``python test_dc.py``

2. Data Import 
Since each database have different build-in tools for data import, we have defined some tools related to data import in the ``tsdb-test/data/load``  directory

3. build project
``cd Tsdb-benchmark/ts-benchmark/``
``sh build.sh``

4. config parametes of database and run the benchmark
``cd Tsdb-benchmark/ts-benchmark/``
``vim run.sh`` (choose database and test mode)
``sh run.sh``

#### Params description

The configuration of TSDBs is shown as follows:

- InfluxDB. We enlarge the default values of some important parameters of the TSM engine for better performance of the system. For example, the parameter wal-fsync- delay is set as "0s", the parameter ``cache-max-memory-size`` is set to 1,048,576,000 bytes, and the parameter cache-snapshot-memory-size is enlarged to “100M” and so on. Maximum memory size is sufficient. The parameter ``max-values-per-tag`` is set as 0 to allow an unlimited number of tag values.
- TimescaleDB. The parameter ``shared-buffers`` is set as 8GB, ``maintenance-work-mem`` is set as 2GB, ``checkpoint-completion-target`` is set as 0.7, ``min_wal`` size is set as 1GB, and max wal size is 2GB. Parameters of PostgreSQL is set based on PgTune.
- Druid. The parameter ``Roll-up`` is set as true, and ``Granu-larity`` is set as hour. For local batch import, the parameter maxRowsPerSegment is set as 10M, maxRowsInMemory is set as 20M, and maxTotalRows is set as 100M. 
- OpenTSDB. The parameter ``tsd-http-request-enable-chunked`` is enabled, ``tsd-http-request-max-chunk`` is set as 32KB, ``tsd-core-auto-create-metrics`` is set as true, and the parameter ``tsd-storage-enable-compaction`` is set to be false to improve the write performance.

#### More

If you have interests in the directed graph construction and the generation via random walk. you can ref to  the ```random_walk.ipynb``` [Quick Open It!](https://nbviewer.jupyter.org/github/dbiir/TS-Benchmark/blob/master/random_walk.ipynb)

More information please ref to [for detail](./documents/时序评测工具使用手册.pdf)
