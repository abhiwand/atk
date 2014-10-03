package com.intel.intelanalytics.engine.spark.frame.parquet

import parquet.io.api.{ Converter, GroupConverter }

/**
 *  Class used by the Parquet libraries to convert a group of objects into a group of primitive objects
 */
class ParquetRecordGroupConverter extends GroupConverter {
  /** called at the beginning of the group managed by this converter */
  override def start(): Unit = {}
  /**
   * call at the end of the group
   */
  override def end(): Unit = {}

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of the field in this group
   * @return the corresponding converter
   */
  override def getConverter(fieldIndex: Int): Converter = {
    new ParquetRecordConverter()
  }
}