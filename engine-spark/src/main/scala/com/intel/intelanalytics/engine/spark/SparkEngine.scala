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
import com.intel.intelanalytics.domain._
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
import spray.json.{JsObject, JsonParser}
import scala.io.{Codec, Source}
import scala.util.control.NonFatal
import org.apache.hadoop.io._
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.{Path => HPath}
import scala.Some
import com.intel.intelanalytics.engine.Row
import scala.util.matching.Regex
import com.typesafe.config.{ConfigResolveOptions, ConfigFactory}
import com.intel.intelanalytics.shared.EventLogging
import com.intel.event.EventContext
import scala.concurrent.duration._
import scala.Some
import com.intel.intelanalytics.domain.DataFrameTemplate
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.domain.Partial

//TODO documentation
//TODO progress notification
//TODO event notification
//TODO pass current user info
class SparkComponent extends EngineComponent
with FrameComponent
with FileComponent
with EventLogging {
  val engine = new SparkEngine {}
  val conf = ConfigFactory.load()
  val sparkHome = conf.getString("intel.analytics.spark.home")
  val sparkMaster = conf.getString("intel.analytics.spark.master")
  val defaultTimeout = conf.getInt("intel.analytics.engine.defaultTimeout").seconds

  //Very simpleminded implementation, not ready for multiple users, for example.
  class SparkEngine extends Engine {

    def config = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("intel-analytics") //TODO: this will probably wind up being a different
      //application name for each user session
      .setSparkHome(sparkHome)


    val runningContexts = new mutable.HashMap[String, SparkContext] with mutable.SynchronizedMap[String, SparkContext] {}

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

    def withMyClassLoader[T](f: => T): T = withContext("se.withClassLoader") {
      val prior = Thread.currentThread().getContextClassLoader
      EventContext.getCurrent.put("priorClassLoader", prior.toString)
      try {
        val loader = this.getClass.getClassLoader
        EventContext.getCurrent.put("newClassLoader", loader.toString)
        Thread.currentThread setContextClassLoader loader
        f
      } finally {
        Thread.currentThread setContextClassLoader prior
      }
    }

    def getPyCommand(function: Partial[Any]): Array[Byte] = {
      function.operation.definition.get.data.getBytes(Codec.UTF8.name)
    }

    def alter(frame: DataFrame, changes: Seq[Alteration]): Unit = withContext("se.alter") {
      ???
    }


    def getLineParser(parser: Partial[Any]): String => Array[String] = {
      parser.operation.name match {
        //TODO: look functions up in a table rather than switching on names
        case "builtin/line/separator" => {
          import DomainJsonProtocol.separatorArgsJsonFormat
          val args = parser.arguments match {
              //TODO: genericize this argument conversion
            case a: JsObject => a.convertTo[SeparatorArgs]
            case a: SeparatorArgs => a
            case x => throw new IllegalArgumentException(
              "Could not convert instance of " + x.getClass.getName + " to  arguments for builtin/line/separator")
          }
          val row = new Row(args.separator)
          s => row(s)
        }
        case x => throw new Exception("Unsupported parser: " + x)
      }
    }

    def appendFile(frame: DataFrame, file: String, parser: Partial[Any]): Future[DataFrame] =
      withContext("se.appendFile") {
        require(frame != null)
        require(file != null)
        require(parser != null)
        future {
          withMyClassLoader {
            //TODO: since we have to look up the real frame anyway, just take an ID as the parameter
            val realFrame = frames.lookup(frame.id).getOrElse(
              throw new IllegalArgumentException(s"No such data frame: ${frame.id}"))
            val parserFunction = getLineParser(parser)
            val location = fsRoot + frames.getFrameDataFile(frame.id)
            val schema = realFrame.schema
            val converter = DataTypes.parseMany(schema.columns.map(_._2).toArray)(_)
            val ctx = context()
            ctx.textFile(fsRoot + "/" + file)
              .map(parserFunction)
              .map(converter)
              .saveAsObjectFile(location)
            frames.lookup(frame.id).getOrElse(
              throw new Exception(s"Data frame ${frame.id} no longer exists or is inaccessible"))
          }
        }
      }

    def clear(frame: DataFrame): Future[DataFrame] = withContext("se.clear") {
      ???
    }

    def create(frame: DataFrameTemplate): Future[DataFrame] = withContext("se.create") {
      future {
        frames.create(frame)
      }
    }

    def delete(frame: DataFrame): Future[Unit] = withContext("se.delete") {
      future {
        frames.drop(frame)
      }
    }

    def getFrames(offset: Int, count: Int): Future[Seq[DataFrame]] = withContext("se.getFrames") {
      future {
        frames.getFrames(offset, count)
      }
    }

    def filter(frame: DataFrame, predicate: Partial[Any]): Future[DataFrame] =
      withContext("se.filter") {
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

    def getRows(id: Identifier, offset: Long, count: Int) = withContext("se.getRows") {
      future {
        val frame = frames.lookup(id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
        val rows = frames.getRows(frame, offset, count)
        rows
      }
    }

    def getFrame(id: SparkComponent.this.Identifier): Future[DataFrame] =
      withContext("se.getFrame") {
        future {
          frames.lookup(id).get
        }
      }
  }

  val files = new HdfsFileStorage {}

  val fsRoot = conf.getString("intel.analytics.fs.root")

  trait HdfsFileStorage extends FileStorage with EventLogging {


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


    override def write(sink: File, append: Boolean): OutputStream = withContext("file.write") {
      val path: HPath = new HPath(fsRoot + sink.path.toString)
      if (append) {
        fs.append(path)
      } else {
        fs.create(path, true)
      }
    }

    override def readRows(source: File, rowGenerator: (InputStream) => RowSource,
                          offsetBytes: Long, readBytes: Long): Unit = withContext("file.readRows") {
      ???
    }

    override def list(source: Directory): Seq[Entry] = withContext("file.list") {
      fs.listStatus(new HPath(fsRoot + frames.frameBase))
        .map {
        case s if s.isDirectory => Directory(path = Paths.get(s.getPath.toString))
        case f if f.isDirectory => File(path = Paths.get(f.getPath.toString), size = f.getLen)
        case x => throw new IOException("Unknown object type in filesystem at " + x.getPath)
      }
    }

    override def read(source: File): InputStream = withContext("file.read") {
      val path: HPath = new HPath(fsRoot + source.path.toString)
      fs.open(path)
    }

    //TODO: switch file methods to strings instead of Path?
    override def copy(source: Path, destination: Path): Unit = withContext("file.copy") {
      ???
    }

    override def move(source: Path, destination: Path): Unit = withContext("file.move") {
      ???
    }


    override def getMetaData(path: Path): Option[Entry] = withContext("file.getMetaData") {
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

    override def delete(path: Path): Unit = withContext("file.delete") {
      fs.delete(new HPath(fsRoot + path.toString), true)
    }

    override def create(file: Path): Unit = withContext("file.create") {
      fs.create(new HPath(fsRoot + file.toString))
    }

    override def createDirectory(directory: Path): Directory = withContext("file.createDirectory") {
      val adjusted = fsRoot + directory.toString
      fs.mkdirs(new HPath(adjusted))
      getMetaData(Paths.get(directory.toString)).get.asInstanceOf[Directory]
    }
  }

  val frames = new SparkFrameStorage {}

  trait SparkFrameStorage extends FrameStorage with EventLogging {

    import spray.json._
    import Rows.Row

    import com.intel.intelanalytics.domain.DomainJsonProtocol._

    override def drop(frame: DataFrame): Unit = withContext("frame.drop") {
      files.delete(Paths.get(getFrameDirectory(frame.id)))
    }

    override def appendRows(startWith: DataFrame, append: Iterable[Row]): Unit =
      withContext("frame.appendRows") {
        ???
      }

    override def removeRows(frame: DataFrame, predicate: (Row) => Boolean): Unit =
      withContext("frame.removeRows") {
        ???
      }

    override def removeColumn(frame: DataFrame): Unit =
      withContext("frame.removeColumn") {
        ???
      }

    override def addColumnWithValue[T](frame: DataFrame, column: Column[T], default: T): Unit =
      withContext("frame.addColumnWithValue") {
        ???
      }

    override def addColumn[T](frame: DataFrame, column: Column[T], generatedBy: (Row) => T): Unit =
      withContext("frame.addColumn") {
        ???
      }

    override def getRows(frame: DataFrame, offset: Long, count: Int): Iterable[Row] =
      withContext("frame.getRows") {
        require(frame != null, "frame is required")
        require(offset >= 0, "offset must be zero or greater")
        require(count > 0, "count must be zero or greater")

        val ctx = engine.context()
        val rdd: RDD[Row] = getFrameRdd(ctx, frame.id)
        val rows = SparkOps.getRows(rdd, offset, count)
        rows
      }

    def getFrameRdd(ctx: SparkContext, id: Long): RDD[Row] = {
      ctx.objectFile[Row](fsRoot + getFrameDataFile(id))
    }

    def getOrCreateDirectory(name: String): Directory = {
      val path = Paths.get(name)
      val meta = files.getMetaData(path).getOrElse(files.createDirectory(path))
      meta match {
        case File(f, s) => throw new IllegalArgumentException(path + " is not a directory")
        case d: Directory => d
      }
    }

    override def create(frame: DataFrameTemplate): DataFrame = withContext("frame.create") {
      val id = nextFrameId()
      val frame2 = new DataFrame(id = id, name = frame.name, schema = frame.schema)
      val meta = File(Paths.get(getFrameMetaDataFile(id)))
      info(s"Saving metadata to $meta")
      val f = files.write(meta)
      try {
        val json: String = frame2.toJson.prettyPrint
        debug(json)
        f.write(json.getBytes(Codec.UTF8.name))
      } finally {
        f.close()
      }
      frame2
    }

    override def lookup(id: Long): Option[DataFrame] = withContext("frame.lookup") {
      val path = getFrameDirectory(id)
      val meta = File(Paths.get(path, "meta"))
      if (files.getMetaData(meta.path).isEmpty) {
        return None
      }
      val f = files.read(meta)
      try {
        val src = Source.fromInputStream(f)(Codec.UTF8).getLines().mkString("")
        val json = JsonParser(src)
        return Some(json.convertTo[DataFrame])
      } finally {
        f.close()
      }
    }

    val idRegex: Regex = "^\\d+$".r

    def getFrames(offset: Int, count: Int): Seq[DataFrame] = withContext("frame.getFrames") {
      files.list(getOrCreateDirectory(frameBase))
        .flatMap {
        case Directory(p) => Some(p.getName(p.getNameCount - 1).toString)
        case _ => None
      }
        .filter(idRegex.findFirstMatchIn(_).isDefined) //may want to extract a method for this
        .flatMap(sid => frames.lookup(sid.toLong))
    }

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

