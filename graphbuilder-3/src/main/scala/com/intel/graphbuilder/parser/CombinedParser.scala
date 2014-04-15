package com.intel.graphbuilder.parser

import com.intel.graphbuilder.elements._

/**
 * Parse rows of input into Edges and Vertices.
 * <p>
 * This could be used if you want to go over the input rows in a single pass.
 * </p>
 */
class CombinedParser(inputSchema: InputSchema, vertexParser: Parser[Vertex], edgeParser: Parser[Edge]) extends Parser[GraphElement](inputSchema) {

  /**
   * Parse a row of data into zero to many GraphElements.
   */
  def parse(row: InputRow): Seq[GraphElement] = {
    edgeParser.parse(row) ++ vertexParser.parse(row)
  }

}
