package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.parser.Parser
import org.apache.spark.rdd.RDD

/**
 * Functions for RDD's that can act as input to GraphBuilder.
 * <p>
 * This is best used by importing GraphBuilderRDDImplicits._
 * </p>
 * @param self input that these functions are applicable to
 */

class ParserRDDFunctions(self: RDD[Seq[_]]) {

  /**
   * Parse the raw rows of input into Vertices
   *
   * @param vertexParser the parser to use
   */
  def parseVertices(vertexParser: Parser[Vertex]): RDD[Vertex] = {
    self.flatMap(row => vertexParser.parse(row))
  }

  /**
   * Parse the raw rows of input into Edges
   *
   * @param edgeParser the parser to use
   */
  def parseEdges(edgeParser: Parser[Edge]): RDD[Edge] = {
    self.flatMap(row => edgeParser.parse(row))
  }

  /**
   * Parse the raw rows of input into GraphElements.
   * <p>
   * This method is useful if you want to make a single pass over the input
   * to parse both Edges and Vertices..
   * </p>
   * @param parser the parser to use
   */
  def parse(parser: Parser[GraphElement]): RDD[GraphElement] = {
    self.flatMap(row => parser.parse(row))
  }

}
