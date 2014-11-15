Giraph Performance Tuning and Benchmarking
==========================================

Apache Giraph is a  Hadoop-based graph-processing framework that is modeled after Google's Pregel.
Giraph jobs are memory-hungry applications because it attempts to load the entire graph in-memory.
Giraph jobs have no reducers, so when it comes to memory settings, try to:

*   Allocate as much memory as you can to the job, but ensure that the total JVM heap size available to
    mappers in $HADOOP_HOME/conf/mapred-site (mapreduce.tasktracker.map.tasks.maximum.* maximum heap
    size in mapred.map.child.java.opts) does not exceed available memory for the node, and leaves room
    for other daemons running on the nodes, e.g., datanode, regionserver, and tasktracker.
    Try to allocate at least 4GB to each map tasks, but this value depends heavily on the size of the graph.
*   The maximum number of workers available is the number of map slots available in cluster - 1 (number of
    nodes * mapreduce.tasktracker.map.tasks.maximum -1).
    One master and the rest are slaves.
*   When writing giraph jobs, try keep your compute function small and minimize vertex memory footprint
    because Giraph tries to load all graph partitions in memory.
    There is a patch for spilling data to disk but it does not appear to be working very well at the moment.

If you run into performance problems, try upgrading to latest version of Giraph.
The latest version might include patches that address your performance concerns.
For example, Facebook submitted a large number of patches that allow Giraph to run at scale `http link <http://www.facebook.com/notes/facebook-engineering/scaling-apache-giraph-to-a-trillion-edges/10151617006153920>`_ .
Some memory optimizations to try are:

*   Enable ByteArrayPartition store that Facebook implemented to optimize memory usage in Giraph using the
    option **-Dgiraph.partitionClass=org.apache.giraph.partition.ByteArrayPartition**.
    This option significantly decreases the amount of memory needed to run Giraph jobs.
    Giraph's default partition store is the SimplePartitionStore (which stores the graph as Java
    objects).
    The ByteArrayPartition store keeps only a representative vertex that serializes/deserializes on the
    fly thereby saving memory.
    Be careful to use this option, some algorithm may fail with this option on.
*   If your messages are stored as complex objects, try enabling the option
    **-Dgiraph.useMessageSizeEncoding=true**.
    *Note: This option will cause your job to crash if the messages are simple objects.*

Other Giraph options available http://giraph.apache.org/options.html.
These options can be set using -D<option=value> in the command for running the Hadoop job.
For large clusters, "The Hadoop Real World Solutions Cookbook, Owens et al. 2013" recommends controlling
the number of messaging threads used by each worker to communicate with other workers, and increasing the
maximum number of messages flushed in bulk.

