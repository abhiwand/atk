package com.intel.graphbuilder.write

import com.intel.graphbuilder.schema.GraphSchema

/**
 * Write the GraphSchema to the underlying Graph database
 */
abstract class SchemaWriter {

  /**
   * Write the schema definition to the underlying Graph database
   */
  def write(graphSchema: GraphSchema)

}