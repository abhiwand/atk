package com.intel.graphbuilder.parser

/**
 * Parser parses objects of type T from rows using the supplied InputSchema.
 */
abstract class Parser[T](inputSchema: InputSchema) extends Serializable {

  /**
   * Parse a row of data into zero to many T
   *
   * Random access is needed so preferably an IndexedSeq[String] is supplied
   */
  def parse(row: Seq[Any]): Seq[T] = {
    parse(new InputRow(inputSchema, row))
  }


  /**
   * Parse a row of data into zero to many T
   */
  def parse(row: InputRow): Seq[T]
}