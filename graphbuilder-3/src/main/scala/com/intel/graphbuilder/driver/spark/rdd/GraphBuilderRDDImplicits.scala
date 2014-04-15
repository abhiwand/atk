package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.elements._
import org.apache.spark.rdd.RDD

/**
 * These implicits can be imported to add GraphBuilder related functions to RDD's
 */
object GraphBuilderRDDImplicits {

  /**
   * Functions applicable to RDD's that can be input to GraphBuilder Parsers
   */
  implicit def inputRDDToParserRDDFunctions(rdd: RDD[Seq[_]]) = new ParserRDDFunctions(rdd)

  /**
   * Functions applicable to Vertex RDD's
   */
  implicit def vertexRDDToVertexRDDFunctions(rdd: RDD[Vertex]) = new VertexRDDFunctions(rdd)

  /**
   * Functions applicable to Edge RDD's
   */
  implicit def edgeRDDToEdgeRDDFunctions(rdd: RDD[Edge]) = new EdgeRDDFunctions(rdd)

  /**
   * Functions applicable to GraphElement RDD's
   */
  implicit def graphElementRDDToGraphElementRDDFunctions(rdd: RDD[GraphElement]) = new GraphElementRDDFunctions(rdd)

}
