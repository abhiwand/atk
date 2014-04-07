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

package com.intel.intelanalytics.engine.spark

import org.apache.spark.{SparkContext, SparkConf, Accumulator}
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap}
import org.apache.spark.broadcast.Broadcast
import scala.collection.Set
import scala.Predef
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.engine.RowFunction
import com.intel.intelanalytics.domain.DataFrame
import scala.concurrent._
import ExecutionContext.Implicits.global
import java.nio.file.Path
import java.io.InputStream
import com.intel.intelanalytics.engine.Rows.RowSource
import scala.collection.{mutable}

class SparkComponent extends EngineComponent with FrameComponent with FileComponent {
  val engine = new SparkEngine {}
  val sparkHome = System.getProperty("spark.home", "c:\\temp")
  val sparkMaster = System.getProperty("spark.master", "local[4]")

  //Very simpleminded implementation, not ready for multiple users, for example.
  class SparkEngine extends Engine {

    def config = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("tribeca")
      .setSparkHome(sparkHome)

    def context() = {
      withMyClassLoader {
        new SparkContext(config = config)
      }
    }

    def withMyClassLoader[T](f: => T): T = {
      val prior = Thread.currentThread().getContextClassLoader
      try {
        Thread.currentThread() setContextClassLoader (this.getClass.getClassLoader)
        f
      } finally {
        Thread.currentThread().setContextClassLoader(prior)
      }
    }

    def getRddForId(id: Long): RDD[Array[Byte]] = {
      context().textFile("frame_" + id + ".txt").map(line => line.getBytes("UTF-8"))
    }

    def getPyCommand(function: RowFunction[Boolean]): Array[Byte] = {
      function.definition.getBytes("UTF-8")
    }

    def alter(frame: DataFrame, changes: Seq[SparkComponent.this.Alteration]): Unit = ???

    def appendFile(frame: DataFrame, file: String, parser: Functional): Future[DataFrame] = ???

    def clear(frame: DataFrame): Future[DataFrame] = ???

    def create(frame: DataFrame): Future[DataFrame] = {
      future {
        //this implementation is temporary
        val fid = {val temp = id; id+=1;temp}
        val newf = frame.copy(id = Some(fid))
        frameStore += (fid -> newf)
        newf
      }
    }

    //temporary
    var id = 1L
    //temporary
    val frameStore : mutable.Map[Long,DataFrame] = new mutable.HashMap[Long,DataFrame]()

    def delete(frame: DataFrame): Future[Unit] = ???

    def filter(frame: DataFrame, predicate: RowFunction[Boolean]): Future[DataFrame] = {
      future {
        val id = frame.id.getOrElse(throw new IllegalArgumentException("Data frame ID is required"))

        val rdd = getRddForId(id)
        val command = getPyCommand(predicate)
        val pythonExec = "python" //TODO: take from env var or config
        val environment = System.getenv() //TODO - should be empty instead?
        val pyRdd = new EnginePythonRDD[Array[Byte]](
            rdd, command = command, System.getenv(),
            new JArrayList, preservePartitioning = true,
            pythonExec = pythonExec,
            broadcastVars = new JArrayList[Broadcast[Array[Byte]]](),
            accumulator = new Accumulator[JList[Array[Byte]]](
              initialValue = new JArrayList[Array[Byte]](),
              param = null)
          )
        pyRdd.map(bytes => new String(bytes, "UTF-8")).saveAsTextFile("frame_" + id + "_drop.txt")
        frame
      }
    }

    def getFrame(id: SparkComponent.this.Identifier): Future[DataFrame] = ???
  }

  val files = new SparkFileStorage {}

  trait SparkFileStorage extends FileStorage {
    override def write(sink: File, append: Boolean): Unit = ???

    override def readRows(source: File, rowGenerator: (InputStream) => RowSource, offsetBytes: Long, readBytes: Long): Unit = ???

    override def list(source: Directory): Seq[Entry] = ???

    override def read(source: File): Iterable[Byte] = ???

    override def copy(source: Path, destination: Path): Unit = ???

    override def move(source: Path, destination: Path): Unit = ???

    override def getMetaData(path: Path): Option[Entry] = ???

    override def delete(path: Path): Unit = ???

    override def create(entry: Entry): Unit = ???
  }

  val frames = new SparkFrameStorage {}

  trait SparkFrameStorage extends FrameStorage {
    override def drop(frame: Frame): Unit = ???

    override def appendRows(startWith: Frame, append: Iterable[Row]): Unit = ???

    override def removeRows(frame: Frame, predicate: (Row) => Boolean): Unit = ???

    override def removeColumn(frame: Frame): Unit = ???

    override def addColumnWithValue[T](frame: Frame, column: Column[T], default: T): Unit = ???

    override def addColumn[T](frame: Frame, column: Column[T], generatedBy: (Row) => T): Unit = ???

    override def scan(frame: Frame): Iterable[Row] = ???

    override def create(frame: Frame): Unit = ???

    override def compile[T](func: RowFunction[T]): (Row) => T = ???
  }

}

