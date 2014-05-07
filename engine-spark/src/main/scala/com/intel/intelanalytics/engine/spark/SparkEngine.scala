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

import org.apache.spark.{ExceptionFailure, SparkContext, SparkConf, Accumulator}
import org.apache.spark.api.python._
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap}
import org.apache.spark.broadcast.Broadcast
import scala.collection.{mutable, Set}
import com.intel.intelanalytics.domain._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.{User, DataFrameTemplate, DataFrame}
import com.intel.intelanalytics.domain.{GraphTemplate, Graph, DataFrameTemplate, DataFrame}
import scala.concurrent._
import ExecutionContext.Implicits.global
import java.nio.file.Paths
import java.io.{IOException, OutputStream, InputStream}
import com.intel.intelanalytics.engine.Rows.RowSource
import java.util.concurrent.atomic.AtomicLong
import org.apache.hadoop.fs.{LocalFileSystem, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.nio.file.Path
import spray.json.{JsObject, JsonParser}
import scala.io.{Codec, Source}
import scala.collection.mutable.{HashSet, ListBuffer, ArrayBuffer, HashMap}
import scala.io.{Codec, Source}
import org.apache.hadoop.fs.{Path => HPath}
import scala.Some
import com.intel.intelanalytics.engine.Row
import scala.util.matching.Regex
import com.typesafe.config.{ConfigResolveOptions, ConfigFactory}
import com.intel.event.EventContext
import scala.concurrent.duration._
import com.intel.intelanalytics.domain.Partial
import com.intel.intelanalytics.repository.{SlickMetaStoreComponent, DbProfileComponent, MetaStoreComponent}
import scala.slick.driver.H2Driver
import scala.util.{Success, Failure, Try}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerStageCompleted
import scala.Some
import com.intel.intelanalytics.domain.DataFrameTemplate
import com.intel.intelanalytics.domain.DataFrame
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.engine.{SparkProgressListener, TestListener}
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.EventLogging

//TODO documentation
//TODO progress notification
//TODO event notification
//TODO pass current user info
class SparkComponent extends EngineComponent
                      with FrameComponent
                      with CommandComponent
                      with FileComponent
                      with DbProfileComponent
                      with SlickMetaStoreComponent
                      with EventLogging {

  val engine = new SparkEngine {}
  lazy val conf = ConfigFactory.load()
  lazy val sparkHome = conf.getString("intel.analytics.spark.home")
  lazy val sparkMaster = conf.getString("intel.analytics.spark.master")
  lazy val defaultTimeout = conf.getInt("intel.analytics.engine.defaultTimeout").seconds
  lazy val connectionString = conf.getString("intel.analytics.metastore.connection.url") match {
    case "" | null => throw new Exception("No metastore connection url specified in configuration")
    case u => u
  }
  lazy val driver = conf.getString("intel.analytics.metastore.connection.driver") match {
    case "" | null => throw new Exception("No metastore driver specified in configuration")
    case d => d
  }

  //TODO: choose database profile driver class from config
  override lazy val profile = withContext("engine connecting to metastore") {
    new Profile(H2Driver, connectionString = connectionString, driver = driver)
  }

  val sparkContextManager = new SparkContextManager(conf, new SparkContextFactory)

  //TODO: only create if the datatabase doesn't already exist. So far this is in-memory only,
  //but when we want to use postgresql or mysql or something, we won't usually be creating tables here.
  metaStore.create()


  class SparkEngine extends Engine {

    def context(implicit user: UserPrincipal) : Context = {
        sparkContextManager.getContext(user.user.api_key)
    }

    def shutdown: Unit = {
      sparkContextManager.cleanup()
    }

    def withMyClassLoader[T](f: => T): T = {
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


    override def getCommands(offset: Int, count: Int): Future[Seq[Command]] = withContext("se.getCommands") {
      future {
        commands.scan(offset, count)
      }
    }

    override def getCommand(id: Identifier): Future[Option[Command]] = withContext("se.getCommand") {
      future {
        commands.lookup(id)
      }
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

      def withCommand[T](command: Command)(block: => T) = {
        commands.complete(command.id, Try {
          block
        })
      }

      def load(arguments: LoadLines[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
        withContext("se.load") {
          require(arguments != null, "arguments are required")
          import DomainJsonProtocol._
          val command: Command = commands.create(new CommandTemplate("load", Some(arguments.toJson.asJsObject)))
          val result: Future[Command] = future {
            withMyClassLoader {
              withContext("se.load.future") {
                withCommand(command) {
                  val realFrame = frames.lookup(arguments.destination).getOrElse(
                    throw new IllegalArgumentException(s"No such data frame: ${arguments.destination}"))
                  val frameId = arguments.destination
                  val parserFunction = getLineParser(arguments.lineParser)
                  val location = fsRoot + frames.getFrameDataFile(frameId)
                  val schema = realFrame.schema
                  val converter = DataTypes.parseMany(schema.columns.map(_._2).toArray)(_)
                  val ctx = context(user)
                  SparkOps.loadLines(ctx.sparkContext, fsRoot + "/" + arguments.source, location, arguments, parserFunction, converter)
                }
                commands.lookup(command.id).get
              }
            }
          }
          (command, result)
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

      def getFrames(offset: Int, count: Int)(implicit p: UserPrincipal): Future[Seq[DataFrame]] = withContext("se.getFrames") {
        future {
          frames.getFrames(offset, count)
        }
      }

      def filter(frame: DataFrame, predicate: Partial[Any])(implicit user: UserPrincipal): Future[DataFrame] =
        withContext("se.filter") {
          future {
            val ctx = context(user).sparkContext //TODO: resource management
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


    def getRows(id: Identifier, offset: Long, count: Int) (implicit user: UserPrincipal) = withContext("se.getRows") {
      future {
        val frame = frames.lookup(id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
        val rows = frames.getRows(frame, offset, count)
        rows
      }
    }

    def getFrame(id: SparkComponent.this.Identifier): Future[Option[DataFrame]] =
      withContext("se.getFrame") {
        future {
          frames.lookup(id)
        }
      }


    def createGraph(graph: GraphTemplate): Future[Graph] = {
      future {
        graphs.createGraph(graph)
      }
    }

    def getGraph(id: SparkComponent.this.Identifier) : Future[Graph] = {
      future {
        graphs.lookup(id).get
      }
    }

    def getGraphs(offset: Int, count: Int) : Future[Seq[Graph]] = {
      future {
        graphs.getGraphs(offset, count)
      }
    }

    def deleteGraph(graph: Graph) : Future[Unit]  = {
      future {
        graphs.drop(graph)
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

    override def getRows(frame: DataFrame, offset: Long, count: Int) (implicit user: UserPrincipal): Iterable[Row] =
      withContext("frame.getRows") {
        require(frame != null, "frame is required")
        require(offset >= 0, "offset must be zero or greater")
        require(count > 0, "count must be zero or greater")
        val ctx = engine.context(user)
        val rdd: RDD[Row] = getFrameRdd(ctx.sparkContext, frame.id)
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

  val commands = new SparkCommandStorage {}

  trait SparkCommandStorage extends CommandStorage {
    val repo = metaStore.commandRepo

    override def lookup(id: Long): Option[Command] =
      metaStore.withSession("se.command.lookup") {
        implicit session =>
          repo.lookup(id)
      }

    override def create(createReq: CommandTemplate): Command =
      metaStore.withSession("se.command.create") {
        implicit session =>

          val created = repo.insert(createReq)
          repo.lookup(created.get.id).getOrElse(throw new Exception("Command not found immediately after creation"))
      }

    override def scan(offset: Int, count: Int): Seq[Command] = metaStore.withSession("se.command.getCommands") {
      implicit session =>
        repo.scan(offset, count)
    }

    override def start(id: Long): Unit = {
      //TODO: set start date
    }

    override def complete(id: Long, result: Try[Unit]): Unit = {
      require(id > 0, "invalid ID")
      require(result != null)
      metaStore.withSession("se.command.complete") {
        implicit session =>
          val command = repo.lookup(id).getOrElse(throw new IllegalArgumentException(s"Command $id not found"))
          if (command.complete) {
            warn(s"Ignoring completion attempt for command $id, already completed")
          }
          //TODO: Update dates
          val changed = result match {
            case Failure(ex) => command.copy(complete = true, error = Some(ex: Error))
            case Success(_) => command.copy(complete = true)
          }
          repo.update(changed)
      }
    }
  }

  val graphs = new SparkGraphStorage {}

  trait SparkGraphStorage extends GraphStorage {

    import spray.json._

    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    //
    // we can't actually use graph builder right now without breaking the build
    // import com.intel.graphbuilder.driver.spark.titan.examples

    override def drop(graph: Graph): Unit = {
      println("DROPPING GRAPH: " + graph.name)
      Unit
    }


    override def createGraph(graph: GraphTemplate): Graph = {
      println("CREATING GRAPH " + graph.name)
      new Graph(1, graph.name)
    }


    override def lookup(id: Long): Option[Graph] = {
      println("DELETING GRAPH " + id)
      None
    }

    def getGraphs(offset: Int, count: Int): Seq[Graph] = {
      println("LISTING " + count + " GRAPHS FROM " + offset)
      List[Graph]()
    }
  }
}

