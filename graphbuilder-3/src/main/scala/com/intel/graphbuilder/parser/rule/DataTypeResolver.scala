package com.intel.graphbuilder.parser.rule

import com.intel.graphbuilder.parser.InputSchema

/**
 * Figure out the dataType of the supplied value using the InputSchema, if needed.
 */
class DataTypeResolver(inputSchema: InputSchema) extends Serializable {

  /**
   * Figure out the dataType of the supplied value using the InputSchema, if needed.
   */
  def get(value: Value): Class[_] = {
    value match {
      case constant: ConstantValue => constant.value.getClass
      case parsed: ParsedValue =>
        val dataType = inputSchema.columnType(parsed.columnName)
        if (dataType == null) {
          throw new RuntimeException("InputSchema did NOT define a dataType for column: " + parsed.columnName
            + ". Please supply the type or don't infer the schema from the rules.")
        }
        dataType
      case compound: CompoundValue => classOf[String]
      case _ => throw new RuntimeException("Unexpected type of value is not yet implemented: " + value)
    }
  }
}
