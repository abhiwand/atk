=============
Configuration
=============

-------------------------
Configuration information
-------------------------

Partitioning
============

Rules of thumb
    Choose a reasonable number of partitions: no smaller than 100, no larger than 10,000 (large cluster)
        Lower bound: at least 2x number of cores in your cluster
            Too few partitions results in
                Less concurrency
                More susceptible to data skew
                Increased memory pressure

        Upper bound: ensure your tasks take at least 100ms (if they are going faster, then you are probably spending more time scheduling tasks than
executing them)
            Too many partitions results in
                Time wasted spent scheduling
                If you choose a number way too high then more time will be spent scheduling than executing

            10,000 would be way too big for a 4 node cluster

        Notes

.. ifconfig:: internal_docs

            Graph builder could not complete 1GB Netflix graph with less than 60 partitions - about 90 was optimal (larger needed for large data)
            Graph builder ran into issues with partition size larger than 2000 on 4 node cluster with larger data sizes

References
==========

Spark Docs
    http://spark.apache.org/docs/0.9.0/configuration.htmlhttps://securewiki.ith.intel.com/images/icons/linkext7.gif
    http://spark.apache.org/docs/0.9.0/tuning.htmlhttps://securewiki.ith.intel.com/images/icons/linkext7.gif

Nice thread on how Shuffle works in Spark,
    http://apache-spark-user-list.1001560.n3.nabble.com/How-does-shuffle-work-in-spark-td584.html


