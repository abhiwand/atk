package com.intel.graphbuilder.graph.titan

import com.thinkaurelius.titan.hadoop.formats.titan_050.cassandra.CachedTitanCassandraRecordReader
import com.thinkaurelius.titan.hadoop.formats.titan_050.hbase.CachedTitanHBaseRecordReader
import org.apache.spark.scheduler.{ SparkListenerApplicationEnd, SparkListener }

/**
 * Ensures clean shut down by invalidating all entries in the Titan graph cache
 * when the spark application shuts down.
 */
class TitanGraphCacheListener() extends SparkListener {
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    System.out.println("Invalidating Titan graph cache:")
    TitanGraphConnector.invalidateGraphCache()
  }
}

/**
 * Ensures clean shut down by invalidating all entries in the Titan/Hadoop HBase graph cache
 * when the spark application shuts down.
 */
class TitanHadoopHBaseCacheListener() extends SparkListener {

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    System.out.println("Invalidating Titan/Hadoop HBase graph cache:")
    CachedTitanHBaseRecordReader.invalidateGraphCache()
  }
}

/**
 * Ensures clean shut down by invalidating all entries in the Titan/Hadoop Cassandra graph cache
 * when the spark application shuts down.
 */
class TitanHadoopCassandraCacheListener() extends SparkListener {
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    System.out.println("Invalidating Titan graph cache:")
    CachedTitanCassandraRecordReader.invalidateGraphCache()
  }
}