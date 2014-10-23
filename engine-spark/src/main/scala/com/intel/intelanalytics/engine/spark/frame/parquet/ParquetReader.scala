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

import java.nio.charset.Charset

import com.intel.intelanalytics.engine.spark.HdfsFileStorage
import org.apache.hadoop.fs.{ Path, FileSystem }
import parquet.column.{ ColumnReadStore, ColumnReader, ColumnDescriptor }
import parquet.column.page.PageReadStore
import parquet.hadoop.{ Footer, ParquetFileReader }
import parquet.hadoop.metadata.ParquetMetadata
import parquet.io.ParquetDecodingException
import parquet.schema.PrimitiveType.PrimitiveTypeName
import parquet.schema.MessageType
import scala.collection.JavaConverters._

import scala.collection.mutable.{ ArrayBuffer, ListBuffer }

/**
 * A class that will read data from parquet files located at a specified path.
 * @param path path containing the parquet files
 * @param fileStorage fileStorage object containing locations for stored RDDs
 */
class ParquetReader(val path: Path, fileStorage: HdfsFileStorage, parquetApiFactory: ParquetApiFactory) {
  def this(path: Path, fileStorage: HdfsFileStorage) = this(path, fileStorage, new ParquetApiFactory(fileStorage.configuration))
  private[parquet] val utf8 = Charset.forName("UTF-8")
  private val decoder = utf8.newDecoder()
  private val ParquetFileOrderPattern = ".*part-r-(\\d*)\\.parquet".r

  /**
   * take from this objects path data from parquet files as a list of arrays.
   * @param count total rows to be included in the new RDD
   * @param offset rows to be skipped before including rows in the new RDD
   * @param limit limit on number of rows to be included in the new RDD
   */
  def take(count: Long, offset: Long = 0, limit: Option[Long]): List[Array[Any]] = {
    val files: List[Footer] = getFiles()
    val capped = limit match {
      case None => count
      case Some(l) => Math.min(count, l)
    }
    var currentOffset: Long = 0
    var currentCount: Long = 0

    val result: ListBuffer[Array[Any]] = new ListBuffer[Array[Any]]
    for (
      file <- files if currentCount < capped
    ) {
      val metaData = file.getParquetMetadata
      val schema = metaData.getFileMetaData.getSchema;

      val reader = this.parquetApiFactory.newParquetFileReader(file.getFile, metaData.getBlocks, schema.getColumns)
      var store: PageReadStore = reader.readNextRowGroup()
      while (store != null && currentCount < capped) {
        if ((currentOffset + store.getRowCount) > offset) {
          val start: Long = math.max(offset - currentOffset, 0)
          val amountToTake: Long = math.min(store.getRowCount - start, capped - currentCount)

          val crstore = this.parquetApiFactory.newColumnReadStore(store, schema)

          val pageReadStoreResults = takeFromPageReadStore(schema, crstore, amountToTake.toInt, start.toInt)
          result ++= pageReadStoreResults
          currentCount = currentCount + pageReadStoreResults.size
        }

        currentOffset = currentOffset + store.getRowCount.toInt
        store = reader.readNextRowGroup()
      }
    }
    result.toList
  }

  /**
   * Read data from a single page
   * @param schema Parquet schema describing columns in this page
   * @param store ColumnReadStore for this page
   * @param count number of rows to take from page
   * @param offset number of rows to skip in this page
   */
  private[parquet] def takeFromPageReadStore(schema: MessageType, store: ColumnReadStore, count: Int, offset: Int): List[Array[Any]] = {
    val result = new ArrayBuffer[Array[Any]]()
    val columns = schema.getColumns.asScala
    val columnslength = columns.size

    columns.zipWithIndex.foreach {
      case (col: ColumnDescriptor, columnIndex: Int) => {
        val dMax = col.getMaxDefinitionLevel
        val creader = store.getColumnReader(col)

        if (offset > 0) {
          0.to(offset - 1).foreach(_ => {
            if (creader.getCurrentDefinitionLevel == dMax)
              creader.skip()
            creader.consume()
          })
        }

        0.to(count - 1).foreach(i => {
          val expectedSize = i

          if (result.size <= expectedSize) {
            result += new Array[Any](columnslength)
          }
          val dlvl = creader.getCurrentDefinitionLevel
          val value = if (dlvl == dMax) {
            getValue(creader, col.getType)
          }
          else {
            null
          }
          result(expectedSize)(columnIndex) = value
          creader.consume()
        })

      }
    }
    result.toList
  }

  /**
   * Returns a sequence of Paths that will be evaluated for finding the required rows
   * @return  List of FileStatus objects corresponding to grouped parquet files
   */
  private[parquet] def getFiles(): List[Footer] = {
    def fileHasRows(metaData: ParquetMetadata): Boolean = {
      metaData.getBlocks.asScala.map(b => b.getRowCount).sum > 0
    }
    val fs: FileSystem = fileStorage.fs
    if (!fs.isDirectory(path)) {
      val metadata = ParquetFileReader.readFooter(fileStorage.configuration, fs.getFileStatus(path))
      List(new Footer(path, metadata))
    }
    else {
      def getFileNumber(path: Path): Int = {
        path.getName match {
          case ParquetFileOrderPattern(i) => i.toInt
          case _ => -1
        }
      }

      val metadataPath: Path = new Path(this.path, "_metadata")
      val metaDataFileStatus = fileStorage.fs.getFileStatus(metadataPath)
      val summaryList = parquetApiFactory.getFooterList(metaDataFileStatus)
      val filteredList = summaryList.filter(f => fileHasRows(f.getParquetMetadata))

      val sortedList = filteredList.sortWith((f1, f2) => {
        getFileNumber(f1.getFile) < getFileNumber(f2.getFile)
      })
      sortedList
    }
  }

  /**
   * retrieve a single value from the parquet file
   * @param creader ColumnReader for parquet file set to needed page
   * @param typeName The parquet type for the data in this column
   */
  private[parquet] def getValue(creader: ColumnReader, typeName: PrimitiveTypeName): Any = {
    typeName match {
      case PrimitiveTypeName.INT32 => creader.getInteger
      case PrimitiveTypeName.INT64 => creader.getLong
      case PrimitiveTypeName.BOOLEAN => creader.getBoolean
      case PrimitiveTypeName.FLOAT => creader.getFloat
      case PrimitiveTypeName.DOUBLE => creader.getDouble
      case _ => {
        val binary = creader.getBinary
        val data = binary.getBytes
        if (data == null)
          ""
        else {
          val buffer = decoder.decode(binary.toByteBuffer)
          buffer.toString
        }
      }
    }
  }

}
