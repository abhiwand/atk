HBase Performance Tuning
========================

Two comprehensive sources for performance tuning in HBase are:

*   `HBase reference guide <http://hbase.apache.org/book.html#important_configurations>`_

*   `The HBase Administration Cookbook`_

Some of the recommendations for production clusters are detailed below.
Remember to sync these changes to all servers in the cluster.

1.  Increase ulimit settings.
    HBase is a database and opens a lot of file descriptors and threads.
    The default Linux setting for maximum number of file descriptors for a user is 1024 which is too low.
    The number of file descriptors (nofile) and open processes for the user running the Hadoop/HBase daemons
    should be set north of 10k otherwise the system will experience problems under heavy load.
    Instructions for changing */etc/security/limits.conf* are available here:
    http://hbase.apache.org/book.html#ulimit

#.  Garbage collection.
    Pauses due to garbage collection can impact performance.
    For latency-sensitive applications, Cloudera recommends **-XX:+UseParNewGC** and
    **-XX:+UseConcMarkSweepGC** in *hbase-env.sh*.
    The MemStore-Local Allocation buffers (MSLAB) which reduce heap fragmentation under heavy write-load are
    enabled by default in HBase 0.94.
    The MSLAB setting is **hbase.hregion.memstore.mslab.enabled** in *hbase-site.xml*.
    http://blog.cloudera.com/blog/2011/02/avoiding-full-gcs-in-hbase-with-memstore-local-allocation-buffers-part-1

#.  Enable column family compression.
    Compression boosts performance by reducing the size of store files thereby reducing I/O
    http://hbase.apache.org/book.html#compression.
    The Snappy library from Google is faster than LZO and easier to install.
    Instructions for installing Snappy are available at:
    http://hbase.apache.org/book.html#d0e24641
    Once installed, turn on column family compression when creating the table, for example, **create
    'mytable', {NAME => 'mycolumnfamily1', COMPRESSION => 'SNAPPY'}**

#.  Avoid update blocking for write-heavy workloads.
    If the cluster has enough memory, increase **hbase.hregion.memstore.block.multiplier** and
    **hbase.hstore.blockingStoreFiles** in *hbase-site.xml* to avoid blocking in write-heavy workloads.
    This setting needs to be tuned carefully because it increases the number of files to compact.
    See `the HBase Administration Cookbook`_ (Chapter 9)

#.  Increase region server handler count.
    If the workload has many concurrent clients, increasing the number of handlers can improve performance.
    However, too many handlers will lead to out of memory errors.
    See `the HBase Administration Cookbook`_ (Chapter 9)

Other recommendations include turning off automatic compactions and doing it manually.
http://hbase.apache.org/book.html#managed.compactions

HBase Benchmarking
==================

The most commonly used HBase benchmark is the Yahoo Cloud Serving Benchmarking (YCSB).
YCSB has a number of core workloads that insert, read, update, and scan the generated HBase table. 

Install and run YCSB as follows:

1.  Install YCSB.
    You need to ensure that versions of sl4j and the HBase client in YCSB and HBase are the same to avoid
    compilation issues.
    Instructions on compiling YCSB for HBase are available here:
    http://fendertech.blogspot.com/2013/09/ycsb-on-hbase.html
2.  Create HBase table for benchmark in YCSB, e.g., create 'usertable', { NAME => 'values', COMPRESSION =>
    'SNAPPY' }
3.  Instructions for loading the table, and running workloads are available here:
    https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload (See Steps 3 to 6)

There are 6 different core workloads implemented in YCSB (e.g., read-mostly, update-heavy, scan short
ranges).
The recommended sequence of instructions for running the workloads is available here:
https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads


.. _The HBase administration cookbook: https://www.safaribooksonline.com/library/view/hbase-administration-cookbook/9781849517140/
