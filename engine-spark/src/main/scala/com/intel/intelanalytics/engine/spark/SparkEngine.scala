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
import com.intel.intelanalytics.domain.{DataFrameTemplate, DataFrame}
import com.intel.intelanalytics.engine.RowFunction
import scala.concurrent._
import ExecutionContext.Implicits.global
import java.nio.file.{Paths, Path}
import java.io.{IOException, OutputStream, ByteArrayInputStream, InputStream}
import com.intel.intelanalytics.engine.Rows.RowSource
import scala.collection.{mutable}
import java.util.concurrent.atomic.AtomicLong
import resource._
import org.apache.hadoop.fs.{FSDataInputStream, LocalFileSystem, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.nio.file.Path
import spray.json.JsonParser
import scala.io.{Codec, Source}
import scala.util.control.NonFatal
import org.apache.hadoop.io._
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.{Path => HPath}
import scala.Some
import com.intel.intelanalytics.engine.Row
import scala.util.matching.Regex
import com.typesafe.config.{ConfigResolveOptions, ConfigFactory}

//TODO logging
//TODO error handling
//TODO documentation
//TODO progress notification
//TODO event notification
//TODO pass current user info
class SparkComponent extends EngineComponent with FrameComponent with FileComponent {
  val engine = new SparkEngine {}
  val conf = ConfigFactory.load()
  val sparkHome = conf.getString("intel.analytics.spark.home")
  val sparkMaster = conf.getString("intel.analytics.spark.master")

  //Very simpleminded implementation, not ready for multiple users, for example.
  class SparkEngine extends Engine {

    def config = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("intel-analytics") //TODO: this will probably wind up being a different
                                      //application name for each user session
      .setSparkHome(sparkHome)


    val runningContexts = new mutable.HashMap[String,SparkContext] with mutable.SynchronizedMap[String,SparkContext] {}
    //TODO: how to run jobs as a particular user
    //TODO: Decide on spark context life cycle - should it be torn down after every operation,
    //or left open for some time, and reused if a request from the same user comes in?
    //Is there some way of sharing a context across two different Engine instances?
    def context() = {
      runningContexts.get("user") match {
        case Some(ctx) => ctx
        case None => {
          val ctx = withMyClassLoader {
            new SparkContext(config = config.setAppName("intel-analytics:user"))
          }
          runningContexts += ("user" -> ctx)
          ctx
        }
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

    def getPyCommand(function: RowFunction[Boolean]): Array[Byte] = {
      function.definition.getBytes(Codec.UTF8.name)
    }

    def alter(frame: DataFrame, changes: Seq[Alteration]): Unit = ???

    def getLineParser(parser: Functional): String => Array[String] = {
      parser.language match {
        case "builtin" => parser.definition match {
          case "line/csv" => (s: String) => {
            val row = new Row(',')
            row.apply(s)
          }
          case p => throw new Exception("Unsupported parser: " + p)
        }
        case lang => throw new Exception("Unsupported language: " + lang)
      }
    }

    def appendFile(frame: DataFrame, file: String, parser: Functional): Future[DataFrame] = {
      require(frame != null)
      require(file != null)
      require(parser != null)
      future {
        withMyClassLoader {
          val parserFunction = getLineParser(parser)
          val location = fsRoot + frames.getFrameDataFile(frame.id)
          val ctx = context()
            ctx.textFile(fsRoot + "/" + file)
              .map(parserFunction)
              //TODO: type conversions based on schema
              .map(strings => strings.map(s => s.getBytes))
              .saveAsObjectFile(location)
            frames.lookup(frame.id).getOrElse(
              throw new Exception(s"Data frame ${frame.id} no longer exists or is inaccessible"))
        }
      }
    }

    def clear(frame: DataFrame): Future[DataFrame] = ???

    def create(frame: DataFrameTemplate): Future[DataFrame] = {
      future {
        frames.create(frame)
      }
    }

    def delete(frame: DataFrame): Future[Unit] = {
      future {
        frames.drop(frame)
      }
    }

    def getFrames(offset: Int, count: Int): Future[Seq[DataFrame]] = {
      future {
        frames.getFrames(offset, count)
      }
    }

    def filter(frame: DataFrame, predicate: RowFunction[Boolean]): Future[DataFrame] = {
      future {
        val ctx = context() //TODO: resource management
        val rdd = frames.getFrameRdd(ctx, frame.id)
        val command = getPyCommand(predicate)
        val pythonExec = "python" //TODO: take from env var or config
        val environment = System.getenv() //TODO - should be empty instead?
//        val pyRdd = new EnginePythonRDD[Array[Byte]](
//            rdd, command = command, System.getenv(),
//            new JArrayList, preservePartitioning = true,
//            pythonExec = pythonExec,
//            broadcastVars = new JArrayList[Broadcast[Array[Byte]]](),
//            accumulator = new Accumulator[JList[Array[Byte]]](
//              initialValue = new JArrayList[Array[Byte]](),
//              param = null)
//          )
//        pyRdd.map(bytes => new String(bytes, Codec.UTF8.name)).saveAsTextFile("frame_" + frame.id + "_drop.txt")
        frame
      }
    }

    def getRows(id: Identifier, offset: Long, count: Int) = {
      future {
        val frame = frames.lookup(id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
        val rows = frames.getRows(frame, offset, count)
        rows
      }
    }

    def getFrame(id: SparkComponent.this.Identifier): Future[DataFrame] = {
      future {
        frames.lookup(id).get
      }
    }
  }

  val files = new HdfsFileStorage {}

  val fsRoot = conf.getString("intel.analytics.fs.root")

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


    override def write(sink: File, append: Boolean): OutputStream = {
      val path: HPath = new HPath(fsRoot + sink.path.toString)
      if (append) {
        fs.append(path)
      } else {
        fs.create(path, true)
      }
    }

    override def readRows(source: File, rowGenerator: (InputStream) => RowSource, offsetBytes: Long, readBytes: Long): Unit = ???

    override def list(source: Directory): Seq[Entry] = {
      fs.listStatus(new HPath(fsRoot + frames.frameBase))
        .map {
        case s if s.isDirectory => Directory(path = Paths.get(s.getPath.toString))
        case f if f.isDirectory => File(path = Paths.get(f.getPath.toString), size = f.getLen)
        case x => throw new IOException("Unknown object type in filesystem at " + x.getPath)
      }
    }

    override def read(source: File): InputStream = {
      val path: HPath = new HPath(fsRoot + source.path.toString)
      fs.open(path)
    }

    //TODO: switch file methods to strings instead of Path?
    override def copy(source: Path, destination: Path): Unit = ???

    override def move(source: Path, destination: Path): Unit = ???

    override def getMetaData(path: Path): Option[Entry] = {
      val hPath: HPath = new HPath(fsRoot + path.toString)
      val exists = fs.exists(hPath)
      if (!exists) {
        None
      } else {
        val status = fs.getStatus(hPath)
        if (status == null || fs.isDirectory(hPath)) {
          Some(Directory(path))
        } else {
          Some(File(path, status.getUsed))
        }
      }
    }

    override def delete(path: Path): Unit = {
      fs.delete(new HPath(fsRoot + path.toString), true)
    }

    override def create(file: Path): Unit = fs.create(new HPath(fsRoot + file.toString))

    override def createDirectory(directory: Path): Directory = {
      val adjusted = fsRoot + directory.toString
      fs.mkdirs(new HPath(adjusted))
      getMetaData(Paths.get(directory.toString)).get.asInstanceOf[Directory]
    }
  }

  val frames = new SparkFrameStorage { }

  trait SparkFrameStorage extends FrameStorage {

    import spray.json._
    import Rows.Row

    import com.intel.intelanalytics.domain.DomainJsonProtocol._

    override def drop(frame: DataFrame): Unit = {
      files.delete(Paths.get(getFrameDirectory(frame.id)))
    }

    override def appendRows(startWith: DataFrame, append: Iterable[Row]): Unit = ???

    override def removeRows(frame: DataFrame, predicate: (Row) => Boolean): Unit = ???

    override def removeColumn(frame: DataFrame): Unit = ???

    override def addColumnWithValue[T](frame: DataFrame, column: Column[T], default: T): Unit = ???

    override def addColumn[T](frame: DataFrame, column: Column[T], generatedBy: (Row) => T): Unit = ???

    override def getRows(frame: DataFrame, offset: Long, count: Int): Iterable[Row] = {
      val ctx = engine.context()
      val rdd: RDD[Array[Array[Byte]]] = getFrameRdd(ctx, frame.id)
      //Brute force until the code below can be fixed
      val rows = rdd.take(offset.toInt + count).drop(offset.toInt)
      //The below fails with a classcast exception saying it can't convert a byteswritable
      //into a Text. Nobody asked it to try to do that cast, so it's a bit mysterious.
      //TODO: Check to see if there's a better way to implement, this might be too slow.
      // Need to cache row counts per partition somewhere.
      //      val counts = rdd.mapPartitionsWithIndex(
      //                            (i:Int, rows:Iterator[Array[Array[Byte]]]) => Iterator.single((i, rows.size)))
      //                      .collect()
      //                      .sortBy(_._1)
      //      val sums = counts.scanLeft((0,0)) { (t1,t2) => (t2._1, t1._2 + t2._2) }
      //                      .drop(1)
      //                      .toMap
      //      val sumsAndCounts = counts.map {case (part, count) => (part, (count, sums(part)))}.toMap
      //      val rows: Seq[Array[Array[Byte]]] = rdd.mapPartitionsWithIndex((i, rows) => {
      //        val (ct: Int, sum: Int) = sumsAndCounts(i)
      //        if (sum < offset || sum - ct > offset + count) {
      //          Iterator.empty
      //        } else {
      //          val start = offset - (sum - ct)
      //          rows.drop(start.toInt).take(count)
      //        }
      //      }).collect()
      rows
    }


    def getFrameRdd(ctx: SparkContext, id: Long): RDD[Array[Array[Byte]]] = {
      ctx.objectFile[Array[Array[Byte]]](fsRoot + getFrameDataFile(id))
    }

    def getOrCreateDirectory(name: String) : Directory = {
      val path = Paths.get(name)
      val meta = files.getMetaData(path).getOrElse(files.createDirectory(path))
      meta match {
        case File(f,s) => throw new IllegalArgumentException(path + " is not a directory")
        case d: Directory => d
      }
    }

    override def create(frame: DataFrameTemplate): DataFrame = {
      val id = nextFrameId()
      val frame2 = new DataFrame(id = id, name = frame.name, schema = frame.schema)
      val meta = File(Paths.get(getFrameMetaDataFile(id)))
      val f = files.write(meta)
      try {
        val json: String = frame2.toJson.prettyPrint
        println("Saving metadata")
        println(json)
        f.write(json.getBytes(Codec.UTF8.name))
      } finally {
        f.close()
      }
      frame2
    }

    override def lookup(id: Long): Option[DataFrame] = {
      val path = getFrameDirectory(id)
      val meta = File(Paths.get(path, "meta"))
      //TODO: uncomment after files.getMetaData implemented
      if (files.getMetaData(meta.path).isEmpty) {
        return None
      }
      val f = files.read(meta)
      try {
        val src = Source.fromInputStream(f)(Codec.UTF8).getLines().mkString("")
        val json = JsonParser(src)
        f.close()
        return Some(json.convertTo[DataFrame])
      } catch {
        case NonFatal(e) => {
          //todo: logging
          println("Problem reading file: " + e)
          e.printStackTrace()
          throw e
        }
      }
    }

    val idRegex: Regex = "^\\d+$".r

    def getFrames(offset: Int, count: Int): Seq[DataFrame] = {
      files.list(getOrCreateDirectory(frameBase))
        .flatMap {
          case Directory(p) => Some(p.getName(p.getNameCount - 1).toString)
          case _ => None
        }
        .filter(idRegex.findFirstMatchIn(_).isDefined)  //may want to extract a method for this
        .flatMap(sid => frames.lookup(sid.toLong))
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

    def getFrameDataFile(id: Long): String = {
      getFrameDirectory(id) + "/data"
    }

    def getFrameMetaDataFile(id: Long): String = {
      getFrameDirectory(id) + "/meta"
    }
  }

}

