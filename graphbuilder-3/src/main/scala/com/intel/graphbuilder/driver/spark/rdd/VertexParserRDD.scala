package com.intel.graphbuilder.driver.spark.rdd

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, Vertex }
import org.apache.spark.{ TaskContext, Partition }
import com.intel.graphbuilder.parser.Parser
import scala.collection.mutable.Map

/**
 * Parse the raw rows of input into Vertices
 *
 * @param vertexParser the parser to use
 */
class VertexParserRDD(prev: RDD[Seq[_]], vertexParser: Parser[Vertex]) extends RDD[Vertex](prev) {

  override def getPartitions: Array[Partition] = firstParent[Vertex].partitions

  /**
   * Parse the raw rows of input into Vertices
   */
  override def compute(split: Partition, context: TaskContext): Iterator[Vertex] = {

    // In some data sets many vertices are duplicates and many of the duplicates are
    // 'near' each other in the parsing process (like Netflix movie data where the
    // rows define both edges and vertices and the rows are ordered by user id). By
    // keeping a map and merging the duplicates that occur in a given split, there
    // will be less to deal with later. This is like a combiner in Hadoop Map/Reduce,
    // it won't remove all duplicates in the final RDD but there will be less to
    // shuffle later.  For input without duplicates, this shouldn't add much overhead.
    val vertexMap = Map[Property, Vertex]()

    firstParent[Seq[_]].iterator(split, context).foreach(row => {
      vertexParser.parse(row).foreach(v => {
        val opt = vertexMap.get(v.gbId)
        if (opt.isDefined) {
          vertexMap.put(v.gbId, v.merge(opt.get))
        }
        else {
          vertexMap.put(v.gbId, v)
        }
      })
    })

    vertexMap.valuesIterator
  }
}
