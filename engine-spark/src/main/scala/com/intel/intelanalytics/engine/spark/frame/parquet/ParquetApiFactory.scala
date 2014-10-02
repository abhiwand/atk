//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.frame.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, Path }
import parquet.column.impl.ColumnReadStoreImpl
import parquet.column.{ ColumnReadStore, ColumnDescriptor }
import parquet.column.page.PageReadStore
import parquet.hadoop.{ Footer, ParquetFileReader }
import parquet.hadoop.metadata.BlockMetaData
import parquet.schema.MessageType
import scala.collection.JavaConverters._

/**
 * Creates instances of parquet library API objects.
 * This factory class is in existence to allow mocking during unit tests
 * @param configuration Hadoop Configuration object
 */
class ParquetApiFactory(val configuration: Configuration) {
  /**
   * Encapsulates the third party ParquetFileReader constructor
   * @param filePath Path to Parquet Hadoop file
   * @param blocks The list of blocks found in this file according to the ParquetFooter
   * @param columns List of columns from a parquet schema
   * @return A ParquetFileReader for said file
   */
  def newParquetFileReader(filePath: Path, blocks: java.util.List[BlockMetaData],
                           columns: java.util.List[ColumnDescriptor]): ParquetFileReader = {
    new ParquetFileReader(this.configuration, filePath, blocks, columns)
  }

  /**
   * Encapsulates the third party ColumnReadStoreImpl constructor
   * @param pageReadStore a readStore representing the current ParquetPage
   * @param schema A Parquet MessageType object representing the schema
   * @return
   */
  def newColumnReadStore(pageReadStore: PageReadStore, schema: MessageType): ColumnReadStore = {
    new ColumnReadStoreImpl(pageReadStore, new ParquetRecordGroupConverter(), schema)
  }

  /**
   *  Encapsulate the third party readSummaryFile static method.
   * @param metaDataFileStatus  Hadoop FileStatus object for the parquet metadata summary file
   * @return A list of ParquetFooter objects for each file that the metadata summary relates to.
   */
  def getFooterList(metaDataFileStatus: FileStatus): List[Footer] = {
    ParquetFileReader.readSummaryFile(this.configuration, metaDataFileStatus).asScala.toList
  }
}