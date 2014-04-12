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
import java.nio.file.{Paths, Path}
import java.io.{OutputStream, ByteArrayInputStream, InputStream}
import com.intel.intelanalytics.engine.Rows.RowSource
import scala.collection.{mutable}
import java.util.concurrent.atomic.AtomicLong
import resource._
import org.apache.hadoop.fs.{LocalFileSystem, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.nio.file.Path
import spray.json.JsonParser
import scala.io.{Codec, Source}
import scala.util.control.NonFatal

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
        Thread.currentThread setContextClassLoader this.getClass.getClassLoader
        f
      } finally {
        Thread.currentThread setContextClassLoader prior
      }
    }

    def getRddForId(id: Long): RDD[Array[Byte]] = {
      context().textFile("frame_" + id + ".txt").map(line => line.getBytes(Codec.UTF8.name))
    }

    def getPyCommand(function: RowFunction[Boolean]): Array[Byte] = {
      function.definition.getBytes(Codec.UTF8.name)
    }

    def alter(frame: DataFrame, changes: Seq[SparkComponent.this.Alteration]): Unit = ???

    def getLineParser(parser: Functional): String => Array[String] = {
      parser.language match {
        case "builtin" => parser.definition match {
          case "line/csv" => (s: String) => {
            s.split(',')
          } //TODO: Return the real parser when Mohit's is finished.
          case p => throw new Exception("Unsupported parser: " + p)
        }
        case lang => throw new Exception("Unsupported language: " + lang)
      }
    }

    def appendFile(frame: DataFrame, file: String, parser: Functional): Future[DataFrame] = {
      require(frame.id.isDefined)
      require(file != null)
      require(parser != null)
      future {
        val parserFunction = getLineParser(parser)
        val location = fsRoot + frames.getFrameDirectory(frame.id.get)
        context().textFile(fsRoot + "/" + file).map(parserFunction).saveAsObjectFile(location)
        frames.lookup(frame.id.get).getOrElse(
          throw new Exception(s"Data frame ${frame.id.get} no longer exists or is inaccessible"))
      }
    }

    def clear(frame: DataFrame): Future[DataFrame] = ???

    def create(frame: DataFrame): Future[DataFrame] = {
      future {
        frames.create(frame)
      }
    }


    def delete(frame: DataFrame): Future[Unit] = {
      future {
        frames.drop(frame)
      }
    }

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
        pyRdd.map(bytes => new String(bytes, Codec.UTF8.name)).saveAsTextFile("frame_" + id + "_drop.txt")
        frame
      }
    }

    def getFrame(id: SparkComponent.this.Identifier): Future[DataFrame] = ???
  }

  val files = new HdfsFileStorage {}

  val fsRoot = System.getProperty("intelanalytics.fs.root", "/home/hadoop")

  trait HdfsFileStorage extends FileStorage {


    val configuration = {
      val hadoopConfig = new Configuration()
      require(hadoopConfig.getClass().getClassLoader == this.getClass.getClassLoader)
      //http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
      hadoopConfig.set("fs.hdfs.impl",
        classOf[DistributedFileSystem].getName)
      hadoopConfig.set("fs.file.impl",
        classOf[LocalFileSystem].getName)
      require(hadoopConfig.getClassByNameOrNull(classOf[LocalFileSystem].getName) != null)
      hadoopConfig
    }


    val fs = FileSystem.get(configuration)

    import org.apache.hadoop.fs.{Path => HPath}

    override def write(sink: File, append: Boolean): OutputStream = {
      val path: HPath = new HPath(fsRoot + sink.path.toString)
      if (append) {
        fs.append(path)
      } else {
        fs.create(path, true)
      }
    }

    override def readRows(source: File, rowGenerator: (InputStream) => RowSource, offsetBytes: Long, readBytes: Long): Unit = ???

    override def list(source: Directory): Seq[Entry] = ???

    override def read(source: File): InputStream = {
      val path: HPath = new HPath(fsRoot + source.path.toString)
      fs.open(path)
    }

    override def copy(source: Path, destination: Path): Unit = ???

    override def move(source: Path, destination: Path): Unit = ???

    override def getMetaData(path: Path): Option[Entry] = ???

    override def delete(path: Path): Unit = ???

    override def create(entry: Entry): Unit = ???
  }

  val frames = new SparkFrameStorage {
  }

  trait SparkFrameStorage extends FrameStorage {

    import spray.json._
    import com.intel.intelanalytics.domain.DomainJsonProtocol._

    override def drop(frame: DataFrame): Unit = ???

    override def appendRows(startWith: DataFrame, append: Iterable[Row]): Unit = ???

    override def removeRows(frame: DataFrame, predicate: (Row) => Boolean): Unit = ???

    override def removeColumn(frame: DataFrame): Unit = ???

    override def addColumnWithValue[T](frame: DataFrame, column: Column[T], default: T): Unit = ???

    override def addColumn[T](frame: DataFrame, column: Column[T], generatedBy: (Row) => T): Unit = ???

    override def scan(frame: DataFrame): Iterable[Row] = ???

    override def create(frame: DataFrame): DataFrame = {
      val id = frame.id.getOrElse(nextFrameId())
      val frame2 = frame.copy(id = Some(id))
      val path = getFrameDirectory(id)
      val meta = File(Paths.get(path, "meta"))
      //todo: resource management
      val f = files.write(meta)
      println("Opened for write")
      f.write(frame2.toJson.prettyPrint.getBytes(Codec.UTF8.name))
      f.close()
//      for (f <- managed(files.write(meta))) {
//        f.write(frame2.toJson.prettyPrint.getBytes(Codec.UTF8.name))
//      }
      frame2
    }

    override def lookup(id: Long): Option[DataFrame] = {
      val path = getFrameDirectory(id)
      val meta = File(Paths.get(path, "meta"))
      //todo: resource management
      val f = files.read(meta)
      println("Opened for read")
      try {
        val src = Source.fromInputStream(f)(Codec.UTF8).getLines().mkString("")
        val json = JsonParser(src)
        f.close()
        return Some(json.convertTo[DataFrame])
      } catch {
        case NonFatal(e) => {
          println("Problem reading file: " + e)
          e.printStackTrace()
          throw e
        }
      }
      //for (f <- managed(files.read(meta))) {

      //}
      None
    }

    override def compile[T](func: RowFunction[T]): (Row) => T = ???

    val frameBase = "/intelanalytics/dataframes"
    //temporary
    var frameId = new AtomicLong(1)

    def nextFrameId() = {
      //Just a temporary implementation, only appropriate for scaffolding.
      frameId.getAndIncrement
    }

    def getFrameDirectory(id: Long): String = {
      val path = Paths.get(s"$frameBase/$id")
      path.toString
    }
  }

}

