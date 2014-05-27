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

import org.apache.spark.{ ExceptionFailure, SparkContext, SparkConf, Accumulator }
import org.apache.spark.api.python._
import java.util.{ List => JList, ArrayList => JArrayList, Map => JMap }
import org.apache.spark.broadcast.Broadcast
import scala.collection.{ mutable, Set }
import com.intel.intelanalytics.domain._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{ RDD, EmptyRDD }
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.{ User, DataFrameTemplate, DataFrame }
import com.intel.intelanalytics.domain.{ GraphTemplate, Graph, DataFrameTemplate, DataFrame }
import scala.concurrent._
import ExecutionContext.Implicits.global
import java.nio.file.{ Paths, Path, Files }
import java.io._
import com.intel.intelanalytics.engine.Rows.RowSource
import java.util.concurrent.atomic.AtomicLong
import org.apache.hadoop.fs.{ LocalFileSystem, FileSystem }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.nio.file.Path
import spray.json.{ JsObject, JsonParser }
import scala.io.{ Codec, Source }
import scala.collection.mutable.{ HashSet, ListBuffer, ArrayBuffer, HashMap }
import scala.io.{ Codec, Source }
import org.apache.hadoop.fs.{ Path => HPath }
import scala.Some
import com.intel.intelanalytics.engine.RowParser
import scala.util.matching.Regex
import com.typesafe.config.{ ConfigResolveOptions, ConfigFactory }
import com.intel.event.EventContext
import scala.concurrent.duration._
import com.intel.intelanalytics.domain.Partial
import com.intel.intelanalytics.repository.{ SlickMetaStoreComponent, DbProfileComponent, MetaStoreComponent }
import scala.slick.driver.H2Driver
import scala.util.{ Success, Failure, Try }
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerStageCompleted
import scala.Some
import com.intel.intelanalytics.domain.DataFrameTemplate
import com.intel.intelanalytics.domain.DataFrame
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.engine.{ SparkProgressListener, ProgressPrinter }
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.domain.DataTypes.DataType

//TODO documentation
//TODO progress notification
//TODO event notification
class SparkComponent extends EngineComponent
    with FrameComponent
    with CommandComponent
    with FileComponent
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging {

  lazy val configuration: SparkEngineConfiguration = new SparkEngineConfiguration()

  val engine = new SparkEngine {}

  //TODO: choose database profile driver class from config
  override lazy val profile = withContext("engine connecting to metastore") {
    new Profile(H2Driver, connectionString = configuration.connectionString, driver = configuration.driver)
  }

  lazy val fsRoot = configuration.fsRoot

  val sparkContextManager = new SparkContextManager(configuration.config, new SparkContextFactory)

  //TODO: only create if the datatabase doesn't already exist. So far this is in-memory only,
  //but when we want to use postgresql or mysql or something, we won't usually be creating tables here.
  metaStore.create()

  class SparkEngine extends Engine {

    def context(implicit user: UserPrincipal): Context = {
      sparkContextManager.getContext(user.user.api_key)
    }

    def shutdown: Unit = {
      sparkContextManager.cleanup()
    }

    /**
     * Execute a code block using the ClassLoader of 'this' SparkEngine
     * rather than the ClassLoader of the currentThread()
     */
    def withMyClassLoader[T](f: => T): T = {
      val prior = Thread.currentThread().getContextClassLoader
      EventContext.getCurrent.put("priorClassLoader", prior.toString)
      try {
        val loader = this.getClass.getClassLoader
        EventContext.getCurrent.put("newClassLoader", loader.toString)
        Thread.currentThread setContextClassLoader loader
        f
      }
      finally {
        Thread.currentThread setContextClassLoader prior
      }
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
          val rowParser = new RowParser(args.separator)
          s => rowParser(s)
        }
        case x => throw new Exception("Unsupported parser: " + x)
      }
    }

    def withCommand[T](command: Command)(block: => T): Unit = {
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

    def renameColumn(arguments: FrameRenameColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.renamecolumn") {
        require(arguments != null, "arguments are required")
        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("renamecolumn", Some(arguments.toJson.asJsObject)))
        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.renamecolumn.future") {
              withCommand(command) {

                val frameID = arguments.frame

                val frame = frames.lookup(frameID).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame: $frameID"))

                val originalcolumns = arguments.originalcolumn.split(",")
                val renamedcolumns = arguments.renamedcolumn.split(",")

                if (originalcolumns.length != renamedcolumns.length)
                  throw new IllegalArgumentException(s"Invalid list of columns: " +
                    s"Lengths of Original and Renamed Columns do not match")

                frames.renameColumn(frame, originalcolumns.zip(renamedcolumns))
              }
              commands.lookup(command.id).get
            }
          }
        }
        (command, result)
      }

    def project(arguments: FrameProject[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.project") {
        require(arguments != null, "arguments are required")
        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("project", Some(arguments.toJson.asJsObject)))
        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.project.future") {
              withCommand(command) {

                val originalFrameID = arguments.originalframe
                val projectedFrameID = arguments.frame

                val originalFrame = frames.lookup(originalFrameID).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame: $originalFrameID"))

                val ctx = context(user).sparkContext
                val columns = arguments.column.split(",")

                val schema = originalFrame.schema
                val location = fsRoot + frames.getFrameDataFile(projectedFrameID)

                val columnIndices = for {
                  col <- columns
                  columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
                } yield columnIndex

                columnIndices match {
                  case invalidColumns if invalidColumns.contains(-1) =>
                    throw new IllegalArgumentException(s"Invalid list of columns: ${arguments.column}")
                  case _ => frames.getFrameRdd(ctx, originalFrameID)
                    .map(row => { for { i <- columnIndices } yield row(i) }.toArray)
                    .saveAsObjectFile(location)
                }

              }
              commands.lookup(command.id).get
            }
          }
        }
        (command, result)
      }

    def decodePythonBase64EncodedStrToBytes(byteStr: String): Array[Byte] = {
      // Python uses different RFC than Java, must correct a couple characters
      // http://stackoverflow.com/questions/21318601/how-to-decode-a-base64-string-in-scala-or-java00
      val corrected = byteStr.map { case '-' => '+'; case '_' => '/'; case c => c }
      new sun.misc.BASE64Decoder().decodeBuffer(corrected)
    }

    /**
     * Create a Python RDD
     * @param frameId source frame for the parent RDD
     * @param py_expression Python expression encoded in Python's Base64 encoding (different than Java's)
     * @param user current user
     * @return the RDD
     */
    private def createPythonRDD(frameId: Long, py_expression: String)(implicit user: UserPrincipal): EnginePythonRDD[String] = {
      withMyClassLoader {
        val ctx = context(user).sparkContext
        val predicateInBytes = decodePythonBase64EncodedStrToBytes(py_expression)

        val baseRdd: RDD[String] = frames.getFrameRdd(ctx, frameId)
          .map(x => x.map(t => t.toString()).mkString(",")) // TODO: we're assuming no commas in the values, isn't this going to cause issues?

        val pythonExec = "python2.7" //TODO: take from env var or config
        val environment = new java.util.HashMap[String, String]()

        val accumulator = ctx.accumulator[JList[Array[Byte]]](new JArrayList[Array[Byte]]())(new EnginePythonAccumulatorParam())

        var broadcastVars = new JArrayList[Broadcast[Array[Byte]]]()

        val pyRdd = new EnginePythonRDD[String](
          baseRdd, predicateInBytes, environment,
          new JArrayList, preservePartitioning = false,
          pythonExec = pythonExec,
          broadcastVars, accumulator)
        pyRdd
      }
    }

    private def persistPythonRDD(pyRdd: EnginePythonRDD[String], converter: Array[String] => Array[Any], location: String): Unit = {
      withMyClassLoader {
        pyRdd.map(s => new String(s).split(",")).map(converter).saveAsObjectFile(location)
      }
    }

    def filter(arguments: FilterPredicate[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.filter") {
        require(arguments != null, "arguments are required")
        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("filter", Some(arguments.toJson.asJsObject)))
        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.filter.future") {
              withCommand(command) {

                val pyRdd = createPythonRDD(arguments.frame, arguments.predicate)

                val location = fsRoot + frames.getFrameDataFile(arguments.frame)

                val realFrame = frames.lookup(arguments.frame).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame: ${arguments.frame}"))
                val schema = realFrame.schema
                val converter = DataTypes.parseMany(schema.columns.map(_._2).toArray)(_)
                persistPythonRDD(pyRdd, converter, location)
              }
              commands.lookup(command.id).get
            }
          }
        }
        (command, result)
      }

    def removeColumn(arguments: FrameRemoveColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.removecolumn") {
        require(arguments != null, "arguments are required")
        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("removecolumn", Some(arguments.toJson.asJsObject)))
        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.removecolumn.future") {
              withCommand(command) {

                val ctx = context(user).sparkContext
                val frameId = arguments.frame
                val columns = arguments.column.split(",")

                val realFrame = frames.lookup(arguments.frame).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame: ${arguments.frame}"))
                val schema = realFrame.schema
                val location = fsRoot + frames.getFrameDataFile(frameId)

                val columnIndices = {
                  for {
                    col <- columns
                    columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
                  } yield columnIndex
                }.sorted.distinct

                columnIndices match {
                  case invalidColumns if invalidColumns.contains(-1) =>
                    throw new IllegalArgumentException(s"Invalid list of columns: ${arguments.column}")
                  case allColumns if allColumns.length == schema.columns.length =>
                    frames.getFrameRdd(ctx, frameId).filter(_ => false).saveAsObjectFile(location)
                  case singleColumn if singleColumn.length == 1 => frames.getFrameRdd(ctx, frameId)
                    .map(row => row.take(singleColumn(0)) ++ row.drop(singleColumn(0) + 1))
                    .saveAsObjectFile(location)
                  case multiColumn => frames.getFrameRdd(ctx, frameId)
                    .map(row => row.zipWithIndex.filter(elem => multiColumn.contains(elem._2) == false).map(_._1))
                    .saveAsObjectFile(location)
                }

                frames.removeColumn(realFrame, columnIndices)
              }
              commands.lookup(command.id).get
            }
          }
        }
        (command, result)
      }

    def addColumn(arguments: FrameAddColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.addcolumn") {
        require(arguments != null, "arguments are required")
        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("addcolumn", Some(arguments.toJson.asJsObject)))
        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.addcolumn.future") {
              withCommand(command) {

                val ctx = context(user).sparkContext
                val frameId = arguments.frame
                val column_name = arguments.columnname
                val column_type = arguments.columntype
                val expression = arguments.expression // Python Wrapper containing lambda expression

                val realFrame = frames.lookup(arguments.frame).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame: ${arguments.frame}"))
                val schema = realFrame.schema
                val location = fsRoot + frames.getFrameDataFile(frameId)

                case class BigColumn[T](override val name: String) extends Column[T]
                val columnObject = new BigColumn(column_name)

                if (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_name) >= 0)
                  throw new IllegalArgumentException(s"Duplicate column name: $column_name")

                // Update the schema
                val newFrame = frames.addColumn(realFrame, columnObject, DataTypes.toDataType(column_type))

                // Update the data
                val pyRdd = createPythonRDD(frameId, expression)
                val converter = DataTypes.parseMany(newFrame.schema.columns.map(_._2).toArray)(_)
                persistPythonRDD(pyRdd, converter, location)

              }
              commands.lookup(command.id).get
            }
          }
        }
        (command, result)
      }

    def getRows(id: Identifier, offset: Long, count: Int)(implicit user: UserPrincipal) = withContext("se.getRows") {
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

    def getGraph(id: SparkComponent.this.Identifier): Future[Graph] = {
      future {
        graphs.lookup(id).get
      }
    }

    def getGraphs(offset: Int, count: Int): Future[Seq[Graph]] = {
      future {
        graphs.getGraphs(offset, count)
      }
    }

    def deleteGraph(graph: Graph): Future[Unit] = {
      future {
        graphs.drop(graph)
      }
    }

    //NOTE: we do /not/ expect to have a separate method for every single algorithm, this will move to a plugin
    //system soon
    def runAls(als: Als[Long]): (Command, Future[Command]) = {
      import spray.json._
      import DomainJsonProtocol._
      val command = commands.create(CommandTemplate("graph/ml/als", Some(als.toJson.asJsObject)))
      withMyClassLoader {
        withContext("se.runAls") {
          val result = future {
            withCommand(command) {
              val graph = graphs.lookup(als.graph).getOrElse(throw new IllegalArgumentException("Graph does not exist"))
              val eConf = ConfigFactory.load("engine.conf").getConfig("engine.algorithm.als")
              val hConf = new Configuration()
              def set[T](hadoopKey: String, arg: Option[T], configKey: String) = {
                hConf.set(hadoopKey, arg.fold(eConf.getString(configKey))(a => a.toString))
              }
              //These parameters are set from engine config values
              val mapping = Seq(
                "giraph.split-master-worker" -> "giraph.SplitMasterWorker",
                "giraph.mapper-memory" -> "mapreduce.map.memory.mb",
                "giraph.mapper-heap" -> "mapreduce.map.java.opts",
                "giraph.zookeeper-external" -> "giraph.zkIsExternal",
                "bias-on" -> "als.biasOn",
                "learning-curve-output-interval" -> "als.learningCurveOutputInterval",
                "max-val" -> "als.maxVal",
                "min-val" -> "als.minVal",
                "bidirectional-check" -> "als.bidirectionalCheck"
              )

              for ((k, v) <- mapping) {
                hConf.set(v, eConf.getString(k))
              }

              //These parameters are set from the arguments passed in, or defaulted from
              //the engine configuration if not passed.
              set("als.lambda", Some(als.lambda), "lambda")
              set("als.maxSuperSteps", als.max_supersteps, "max-supersteps")
              set("als.convergenceThreshold", als.converge_threshold, "convergence-threshold")
              set("als.featureDimension", als.feature_dimension, "feature-dimension")

              //TODO: invoke the giraph algorithm here.

            }
            commands.lookup(command.id).get
          }
          (command, result)
        }
      }
    }
  }

  val files = new HdfsFileStorage {}

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
      }
      else {
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
      }
      else {
        val status = fs.getStatus(hPath)
        if (status == null || fs.isDirectory(hPath)) {
          Some(Directory(path))
        }
        else {
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

    override def removeColumn(frame: DataFrame, columnIndex: Seq[Int]): Unit =
      withContext("frame.removeColumn") {

        val remainingColumns = {
          columnIndex match {
            case singleColumn if singleColumn.length == 1 =>
              frame.schema.columns.take(singleColumn(0)) ++ frame.schema.columns.drop(singleColumn(0) + 1)
            case _ =>
              frame.schema.columns.zipWithIndex.filter(elem => columnIndex.contains(elem._2) == false).map(_._1)
          }
        }
        val newSchema = frame.schema.copy(columns = remainingColumns)
        val newFrame = frame.copy(schema = newSchema)

        val meta = File(Paths.get(getFrameMetaDataFile(frame.id)))
        info(s"Saving metadata to $meta")
        val f = files.write(meta)
        try {
          val json: String = newFrame.toJson.prettyPrint
          debug(json)
          f.write(json.getBytes(Codec.UTF8.name))
        }
        finally {
          f.close()
        }
      }

    override def addColumnWithValue[T](frame: DataFrame, column: Column[T], default: T): Unit =
      withContext("frame.addColumnWithValue") {
        ???
      }
    override def renameColumn[T](frame: DataFrame, name_pairs: Seq[(String, String)]): Unit =
      withContext("frame.renameColumn") {
        val columnsToRename: Seq[String] = name_pairs.map(_._1)
        val newColumnNames: Seq[String] = name_pairs.map(_._2)

        def generateNewColumnTuple(oldColumn: String, columnsToRename: Seq[String], newColumnNames: Seq[String]): String = {
          val columnIndex: Int = columnsToRename.indexOf(oldColumn)
          if (columnIndex > 0)
            return newColumnNames(columnIndex)
          else return oldColumn
        }

        val newColumns = frame.schema.columns.map(col => (generateNewColumnTuple(col._1, columnsToRename, newColumnNames), col._2))

        val newSchema = frame.schema.copy(columns = newColumns)
        val newFrame = frame.copy(schema = newSchema)

        val meta = File(Paths.get(getFrameMetaDataFile(frame.id)))
        info(s"Saving metadata to $meta")
        val f = files.write(meta)
        try {
          val json: String = newFrame.toJson.prettyPrint
          debug(json)
          f.write(json.getBytes(Codec.UTF8.name))
        }
        finally {
          f.close()
        }
        newFrame
      }

    override def addColumn[T](frame: DataFrame, column: Column[T], columnType: DataTypes.DataType): DataFrame =
      withContext("frame.addColumn") {
        val newColumns = frame.schema.columns :+ (column.name, columnType)
        val newSchema = frame.schema.copy(columns = newColumns)
        val newFrame = frame.copy(schema = newSchema)

        val meta = File(Paths.get(getFrameMetaDataFile(frame.id)))
        info(s"Saving metadata to $meta")
        val f = files.write(meta)
        try {
          val json: String = newFrame.toJson.prettyPrint
          debug(json)
          f.write(json.getBytes(Codec.UTF8.name))
        }
        finally {
          f.close()
        }
        newFrame
      }

    override def getRows(frame: DataFrame, offset: Long, count: Int)(implicit user: UserPrincipal): Iterable[Row] =
      withContext("frame.getRows") {
        require(frame != null, "frame is required")
        require(offset >= 0, "offset must be zero or greater")
        require(count > 0, "count must be zero or greater")
        val ctx = engine.context(user)
        val rdd: RDD[Row] = getFrameRdd(ctx.sparkContext, frame.id)
        val rows = SparkOps.getRows(rdd, offset, count)
        rows
      }

    /**
     * Create an RDD from a frame data file.
     * @param ctx spark context
     * @param frameId primary key of the frame record
     * @return the newly created RDD
     */
    def getFrameRdd(ctx: SparkContext, frameId: Long): RDD[Row] = {
      ctx.objectFile[Row](fsRoot + getFrameDataFile(frameId))
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
      }
      finally {
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
      }
      finally {
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

