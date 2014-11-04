Partitioning
============

Rules of thumb:

*   Choose a reasonable number of partitions:

    *   No smaller than 100, no larger than 10,000 (large cluster)

    *   Lower bound:

        *   At least 2x number of cores in your cluster

        *   Too few partitions results in

            *   Less concurrency
            *   More susceptible to data skew
            *   Increased memory pressure

    *   Upper bound: Ensure your tasks take at least 100ms (if they are going faster, then you are probably
        spending more time scheduling tasks than executing them)

        *   Too many partitions results in

            *   Time wasted spent scheduling
            *   If you choose a number way too high then more time will be spent scheduling than executing

        *   10,000 would be way too big for a 4 node cluster

*   Notes:

    *   Graph builder could not complete a 1GB graph with less than 60 partitions - about 90 was optimal
        (larger needed for large data)

    *   Graph builder ran into issues with partition size larger than 2000 on 4 node cluster with larger
        data sizes


