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

import org.apache.spark.SparkContext
import org.apache.spark.api.python._
import java.util.{ List => JList, ArrayList => JArrayList, Map => JMap }
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable
import com.intel.intelanalytics.domain._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{ RDD, EmptyRDD }
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.domain.{ GraphTemplate, Graph }
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import java.nio.file.{ Paths, Path, Files }
import java.io._
import com.intel.intelanalytics.engine.Rows._
import java.util.concurrent.atomic.AtomicLong
import org.apache.hadoop.fs.{ LocalFileSystem, FileSystem }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.nio.file.Path
import spray.json.JsObject
import scala.io.Codec
import scala.io.{ Codec, Source }
import org.apache.hadoop.fs.{ Path => HPath }
import com.intel.intelanalytics.engine.RowParser

import scala.util.matching.Regex
import com.typesafe.config.ConfigResolveOptions
import com.intel.event.EventContext
import com.intel.intelanalytics.domain.Partial
import com.intel.intelanalytics.repository.{ SlickMetaStoreComponent, DbProfileComponent, MetaStoreComponent }
import scala.slick.driver.H2Driver
import scala.util.{ Success, Try }
import org.apache.spark.scheduler.SparkListenerStageCompleted
import scala.Some
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.engine.ProgressPrinter
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.EventLogging

import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import com.intel.graphbuilder.driver.spark.titan.examples.ExamplesUtils
import scala.util.Failure
import scala.collection.JavaConverters._
import com.intel.intelanalytics.domain.LoadLines
import com.intel.intelanalytics.domain.Graph
import com.intel.intelanalytics.domain.DataFrameTemplate
import scala.util.Success
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.domain.Partial
import com.intel.intelanalytics.domain.SeparatorArgs
import com.intel.intelanalytics.domain.CommandTemplate
import com.intel.intelanalytics.domain.Error
import com.thinkaurelius.titan.core.{ TitanGraph, TitanFactory }
import com.tinkerpop.blueprints.{ Direction, Vertex }
import com.intel.graphbuilder.util.SerializableBaseConfiguration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import com.intel.intelanalytics.engine.spark.graph.SparkGraphHBaseBackend

import scala.util.Failure
import scala.Some
import scala.collection.JavaConverters._
import com.intel.intelanalytics.domain.LoadLines
import com.intel.intelanalytics.domain.Graph
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.FrameRemoveColumn
import com.intel.intelanalytics.domain.GraphTemplate
import com.intel.intelanalytics.domain.DataFrameTemplate
import com.intel.intelanalytics.domain.FrameAddColumn
import scala.util.Success
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.domain.Command
import com.intel.intelanalytics.domain.Partial
import com.intel.intelanalytics.domain.SeparatorArgs
import com.intel.intelanalytics.domain.CommandTemplate
import com.intel.intelanalytics.domain.Error
import com.intel.intelanalytics.domain.Als
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphStorage, SparkGraphHBaseBackend }
import com.intel.intelanalytics.domain.DataTypes.DataType

import spray.json._
import com.intel.intelanalytics.domain.DataTypes.DataType
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

  import DomainJsonProtocol._
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

    def withCommand[T](command: Command)(block: => JsObject): Unit = {
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
                val schema = arguments.schema
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

    def renameFrame(arguments: FrameRenameFrame[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.rename_frame") {
        require(arguments != null, "arguments are required")
        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("rename_frame", Some(arguments.toJson.asJsObject)))
        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.rename_frame.future") {
              withCommand(command) {

                val frameID = arguments.frame

                val frame = frames.lookup(frameID).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame: $frameID"))

                val newName = arguments.new_name
                frames.renameFrame(frame, newName)
                JsNull.asJsObject
              }
              commands.lookup(command.id).get
            }
          }
        }
        (command, result)
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
                JsNull.asJsObject
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

                val sourceFrameID = arguments.frame
                val sourceFrame = frames.lookup(sourceFrameID).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame: $sourceFrameID"))

                val projectedFrameID = arguments.projected_frame
                val projectedFrame = frames.lookup(projectedFrameID).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame: $projectedFrameID"))

                val ctx = context(user).sparkContext
                val columns = arguments.columns

                val schema = sourceFrame.schema
                val location = fsRoot + frames.getFrameDataFile(projectedFrameID)

                val columnIndices = for {
                  col <- columns
                  columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
                } yield columnIndex

                if (columnIndices.contains(-1)) {
                  throw new IllegalArgumentException(s"Invalid list of columns: ${arguments.columns.toString()}")
                }

                frames.getFrameRdd(ctx, sourceFrameID)
                  .map(row => { for { i <- columnIndices } yield row(i) }.toArray)
                  .saveAsObjectFile(location)

                val projectedColumns = arguments.new_column_names match {
                  case empty if empty.size == 0 => for { i <- columnIndices } yield schema.columns(i)
                  case _ => {
                    for { i <- 0 until columnIndices.size }
                      yield (arguments.new_column_names(i), schema.columns(columnIndices(i))._2)
                  }
                }
                frames.updateSchema(projectedFrame, projectedColumns.toList)
                JsNull.asJsObject
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

    /**
     * flatten rdd by the specified column
     * @param flattenColumnCommand input specification for column flattening
     * @param user current user
     */
    override def flattenColumn(flattenColumnCommand: FlattenColumn[Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.flattenColumn") {
        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("flattenColumn", Some(flattenColumnCommand.toJson.asJsObject)))
        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.flattenColumn.future") {
              val frameId: Long = flattenColumnCommand.frame
              val realFrame = frames.lookup(frameId).getOrElse(
                throw new IllegalArgumentException(s"No such data frame: ${frameId}"))

              withCommand(command) {
                val ctx = context(user).sparkContext

                /* create a dataframe should take very little time, much less than 10 minutes */
                val newFrame = Await.result(create(DataFrameTemplate(flattenColumnCommand.name)), 10 minutes)
                val rdd = frames.getFrameRdd(ctx, frameId)

                val columnIndex = realFrame.schema.columnIndex(flattenColumnCommand.column)

                val flattenedRDD = SparkOps.flattenRddByColumnIndex(columnIndex, flattenColumnCommand.separator, rdd)

                flattenedRDD.saveAsObjectFile(fsRoot + frames.getFrameDataFile(newFrame.id))
                newFrame.copy(schema = realFrame.schema).toJson.asJsObject
              }
            }
          }
          commands.lookup(command.id).get
        }

        (command, result)
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
                JsNull.asJsObject
              }
              commands.lookup(command.id).get
            }
          }
        }
        (command, result)
      }

    /**
     * join two data frames
     * @param joinCommand parameter contains information for the join operation
     * @param user current user
     */
    override def join(joinCommand: FrameJoin)(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.join") {

        def createPairRddForJoin(joinCommand: FrameJoin, ctx: SparkContext): List[RDD[(Any, Array[Any])]] = {
          val tupleRddColumnIndex: List[(RDD[Rows.Row], Int)] = joinCommand.frames.map {
            frame =>
              {
                val realFrame = frames.lookup(frame._1).getOrElse(
                  throw new IllegalArgumentException(s"No such data frame"))

                val frameSchema = realFrame.schema
                val rdd = frames.getFrameRdd(ctx, frame._1)
                val columnIndex = frameSchema.columns.indexWhere(columnTuple => columnTuple._1 == frame._2)
                (rdd, columnIndex)
              }
          }

          val pairRdds = tupleRddColumnIndex.map {
            t =>
              val rdd = t._1
              val columnIndex = t._2
              rdd.map(p => SparkOps.create2TupleForJoin(p, columnIndex))
          }

          pairRdds
        }

        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("join", Some(joinCommand.toJson.asJsObject)))

        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.join.future") {




              val originalColumns = joinCommand.frames.map {
                frame =>
                  {
                    val realFrame = frames.lookup(frame._1).getOrElse(
                      throw new IllegalArgumentException(s"No such data frame"))

                    realFrame.schema.columns
                  }
              }

              val leftColumns: List[(String, DataType)] = originalColumns(0)
              val rightColumns: List[(String, DataType)] = originalColumns(1)
              val allColumns = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)

              /* create a dataframe should take very little time, much less than 10 minutes */
              val newJoinFrame = Await.result(create(DataFrameTemplate(joinCommand.name)), 10 minutes)

              withCommand(command) {

                //first validate join columns are valid
                val leftOn: String = joinCommand.frames(0)._2
                val rightOn: String = joinCommand.frames(1)._2

                val leftSchema = Schema(leftColumns)
                val rightSchema = Schema(rightColumns)

                require(leftSchema.columnIndex(leftOn) != -1, s"column $leftOn is invalid")
                require(rightSchema.columnIndex(rightOn) != -1, s"column $rightOn is invalid")

                val ctx = context(user).sparkContext
                val pairRdds = createPairRddForJoin(joinCommand, ctx)

                val joinResultRDD = SparkOps.joinRDDs(RDDJoinParam(pairRdds(0), leftColumns.length), RDDJoinParam(pairRdds(1), rightColumns.length), joinCommand.how)
                joinResultRDD.saveAsObjectFile(fsRoot + frames.getFrameDataFile(newJoinFrame.id))
                frames.updateSchema(newJoinFrame, allColumns)
                newJoinFrame.copy(schema = Schema(allColumns)).toJson.asJsObject
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
                JsNull.asJsObject
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
                JsNull.asJsObject
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

    /**
     * Register a graph name with the metadata store.
     * @param graph Metadata for graph creation.
     * @param user IMPLICIT. The user creating the graph.
     * @return Future of the graph to be created.
     */
    def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal) = {
      future {
        withMyClassLoader {
          graphs.createGraph(graph)
        }
      }
    }

    /**
     * Loads graph data into a graph in the database. The source is tabular data interpreted by user-specified  rules.
     * @param arguments Graph construction
     * @param user IMPLICIT. The user loading the graph
     * @return Command object for this graphload and a future
     */
    def loadGraph(arguments: GraphLoad[JsObject, Long, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
      withContext("se.load") {
        require(arguments != null, "arguments are required")
        import spray.json._
        import DomainJsonProtocol._
        val command: Command = commands.create(new CommandTemplate("graphLoad", Some(arguments.toJson.asJsObject)))
        val result: Future[Command] = future {
          withMyClassLoader {
            withContext("se.graphLoad.future") {
              withCommand(command) {

                // validating frames
                arguments.frame_rules.map(frule => frames.lookup(frule.frame).getOrElse(throw new IllegalArgumentException(s"No such data frame: ${frule.frame}")))

                val graph = graphs.loadGraph(arguments)(user)
                graph.toJson.asJsObject
              }

              commands.lookup(command.id).get

            }
          }
        }
        (command, result)
      }

    /**
     * Obtains a graph's metadata from its identifier.
     * @param id Unique identifier for the graph provided by the metastore.
     * @return A future of the graph metadata entry.
     */
    def getGraph(id: SparkComponent.this.Identifier): Future[Graph] = {
      future {
        graphs.lookup(id).get
      }
    }

    /**
     * Get the metadata for a range of graph identifiers.
     * @param offset First graph to obtain.
     * @param count Number of graphs to obtain.
     * @param user IMPLICIT. User listing the graphs.
     * @return Future of the sequence of graph metadata entries to be returned.
     */
    def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal): Future[Seq[Graph]] =
      withContext("se.getGraphs") {
        future {
          graphs.getGraphs(offset, count)
        }
      }

    /**
     * Delete a graph from the graph database.
     * @param graph The graph to be deleted.
     * @return A future of unit.
     */
    def deleteGraph(graph: Graph): Future[Unit] = {
      withContext("se.deletegraph") {
        future {
          graphs.drop(graph)
        }
      }
    }

    //NOTE: we do /not/ expect to have a separate method for every single algorithm, this will move to a plugin
    //system soon
    def runAls(als: Als[Long]): (Command, Future[Command]) = {
      import spray.json._
      import DomainJsonProtocol._
      val command = commands.create(CommandTemplate("graph/ml/als", Some(als.toJson.asJsObject)))
      withContext("se.runAls") {
        withMyClassLoader {
          val result = future {
            withCommand(command) {
              //val graph = graphs.lookup(als.graph).getOrElse(throw new IllegalArgumentException("Graph does not exist"))
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
              //TODO: Use hadoop interfaces rather than subprocess call
              //val job = Job.getInstance(hConf)
              //job.setOutputKeyClass()

              import scala.sys.process._
              //
              //// This uses ! to get the exit code
              //def fileExists(name: String) = Seq("test", "-f", name).! == 0
              //
              //// This uses !! to get the whole result as a string
              //val dirContents = "ls".!!
              //
              //// This "fire-and-forgets" the method, which can be lazily read through
              //// a Stream[String]
              //def sourceFilesAt(baseDir: String): Stream[String] = {
              //  val cmd = Seq("find", baseDir, "-name", "*.scala", "-type", "f")
              //    cmd.lines
              //    }

              info("launching process")
              val cmd = Seq("echo", "hello from sub process!")
              for (l <- cmd.lines) {
                info("Received from process: " + l)
              }
              info("completed process")
              //                Seq(
              //                          "hadoop",
              //                          "jar",
              //                          "/home/ec2-user/cdh_tribeca/IntelAnalytics/target/IntelAnalytics-application-0.8-SNAPSHOT.jar",
              //"org.apache.giraph.GiraphRunner -Dgiraph.zkList=ip-172-31-43-19.us-west-2.compute.internal",
              //"-Dgiraph.SplitMasterWorker=false -Dmapreduce.jobtracker.address=ip-172-31-43-19.us-west-2.compute.internal",
              //"-Dgiraph.titan.input.storage.backend=hbase -Dgiraph.titan.input.storage.hostname=ip-172-31-43-19.us-west-2.compute.internal",
              //"-Dgiraph.titan.input.storage.tablename=netflix -Dgiraph.titan.input.storage.port=2181",
              //"-Dgiraph.titan.input.storage.read-only=false -Dgiraph.titan.input.autotype=none -Dinput.edge.property.key.list=rating",
              //"-Dinput.edge.label.list=rates -Doutput.vertex.property.key.list=als_result,bias -Dvertex.type.property.key=vertexType",
              //"-Dedge.type.property.key=splits -Doutput.vertex.bias=true com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation",
              //"-mc com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation$AlternatingLeastSquaresMasterCompute",
              //"-aw com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation$AlternatingLeastSquaresAggregatorWriter",
              //"-vif  com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4CF",
              //"-vof com.intel.giraph.io.titan.TitanVertexOutputFormatPropertyGraph4CF -op hdfs:///user/ec2-user/als -w 1",
              //"-ca als.maxSupersteps=10  -ca als.convergenceThreshold=0  -ca als.lambda=0.065  -ca als.featureDimension=3 -ca als.biasOn=true",
              //)
              JsNull.asJsObject
            }
            commands.lookup(command.id).get
          }
          (command, result)
        }
      }
    }

    private var titanConnections: Map[Identifier, TitanGraph] = Map.empty

    //TODO: Cleanup these connections at some point.
    protected def connect(graphId: Identifier): TitanGraph = {
      titanConnections.get(graphId) match {
        case Some(g) => g
        case None =>
          //TODO: get these settings from engine.conf or another config
          // Titan Settings
          val titanConfig = new SerializableBaseConfiguration()
          titanConfig.setProperty("storage.backend", "hbase")
          titanConfig.setProperty("storage.tablename", "iagraph-" + graphId)
          //titanConfig.setProperty("storage.backend", "cassandra")
          //titanConfig.setProperty("storage.keyspace", "netflix")
          titanConfig.setProperty("storage.hostname", ExamplesUtils.storageHostname)
          //titanConfig.setProperty("storage.batch-loading", "true")
          titanConfig.setProperty("autotype", "none")
          titanConfig.setProperty("storage.buffer-size", "2048")
          titanConfig.setProperty("storage.attempt-wait", "300")
          titanConfig.setProperty("storage.lock-wait-time", "400")
          titanConfig.setProperty("storage.lock-retries", "15")
          titanConfig.setProperty("storage.idauthority-retries", "30")
          titanConfig.setProperty("storage.write-attempts", "10")
          titanConfig.setProperty("storage.read-attempts", "6")
          titanConfig.setProperty("ids.block-size", "300000")
          titanConfig.setProperty("ids.renew-timeout", "150000")
          val g = TitanFactory.open(titanConfig)
          synchronized {
            titanConnections += (graphId -> g)
          }
          g
      }
    }

    def alsQuery(graphId: Identifier, offset: Int, count: Int, map: Map[String, String]): Iterable[Row] = {

      //TODO: Needs cleanup and verification.

      val queryConf = ConfigFactory.load("engine.conf").getConfig("engine.query.ALSQuery")
      def get(mapName: String, defaultName: String) = {
        map.getOrElse(mapName, queryConf.getString(defaultName))
      }
      val g = connect(graphId)
      val id = map("vertex_id")
      val idProp = get("key_name", "key-name")
      val vertexTypeProp = get("vtype_name", "vertex-type-name")
      val vertexType = get("vtype", "vertex-type")
      val biasOn = get("bias_on", "bias-on").toBoolean
      val propertyList = get("props", "result-property-list").split(";")
      val edgeTypeProp = get("etype_name", "edge-type-name")
      val edgeType = get("edge_type", "edge")
      val featureDimensions = get("feat_dims", "feature-dimensions")
      val leftType = get("left_type", "left-type").toUpperCase
      val rightType = get("right_type", "right-type").toUpperCase
      val leftName = get("left_name", "left-name")
      val rightName = get("right_name", "right-name")
      val vectorValue = get("vector", "vector-value").toBoolean
      val train = get("train", "train")

      val start = g.query().has(idProp, id).has(vertexTypeProp, vertexType).vertices().iterator().next()

      //      val entities = List((leftTypeStr):leftName + '  ', (rightTypeStr):rightName + '  ')
      //    commonStr = 'Top 10 recommendations to '
      //    comments = [(leftTypeStr):commonStr + leftName + ': ', (rightTypeStr):commonStr + rightName + ': ']
      //    //vertexType = v.getProperty(key4VertexType)

      val recommendType = if (vertexType == rightType) {
        leftType
      }
      else {
        rightType
      }

      //    recommendType = rightTypeStr
      //    if (vertexType == rightTypeStr) {
      //        recommendType = leftTypeStr
      //    }
      /*

    println "================" + comments[vertexType] + vertexID + "================"
    list1 = getResults(v, propertyList, vectorValue, biasOn)
    println list1
    */
      val commasAndWhiteSpace = "[\\s,\\t]+".r
      def results(v: Vertex): List[Double] = {
        val length = propertyList.length
        if (length == 0) {
          throw new IllegalArgumentException("no property provided for ML result!")
        }
        else if (vectorValue && biasOn && length != 2) {
          throw new IllegalArgumentException("wrong property length provided for ML result!")
        }

        val bias = if (biasOn) {
          Some(v.getProperty[Double](propertyList.last))
        }
        else {
          None
        }
        val properties = if (vectorValue) {
          commasAndWhiteSpace.split(v.getProperty(propertyList.head)).map(_.toDouble)
        }
        else {
          val valueLength = if (biasOn) {
            length - 1
          }
          else {
            length
          }
          propertyList.take(valueLength).map(p => v.getProperty[Double](p))
        }
        (bias ++ properties).toList
      }

      val list1 = results(start)

      def calculateScore(list2: Seq[Double]) {
        if (biasOn) {
          list1(0) + list2(0) + (list1.drop(1), list2.drop(1)).zipped.map(_ * _).sum
        }
        else {
          (list1, list2).zipped.map(_ * _).sum
        }
      }

      val res = g.query().has(vertexTypeProp).vertices().asScala
        .filter(_.getProperty(vertexTypeProp).toString.toUpperCase() == recommendType)
        .filter(_.getEdges(Direction.OUT).asScala.filter(
          _.getProperty(edgeTypeProp).toString.toUpperCase() == train).isEmpty)
        .map(v2 => {
          val list2 = results(v2)
          val score = calculateScore(list2)
          Array(v2.getProperty(idProp).toString, score)
        })

      res

      /*

    def list = []
    for(Vertex v2 : g.V.filter{(it.getProperty(key4VertexType)).toUpperCase() == recommendType}) {
        list2 = getResults(v2, propertyList, vectorValue, biasOn)
        score = calculateScore(list1, list2, biasOn, featureDimension)
        if (v2.outE.filter{it.getProperty(key4EdgeType).toUpperCase() != trainStr}){
            list.add new recommendation(id:v2.getProperty(key4VertexID), rec:score)
        }
    }
    listSize = list.size()
    if (listSize > 0){
        size = listSize >= 10? 10 : listSize
        sortedlist = list.sort{a,b -> b.rec<=>a.rec}[0..<size]
        (0..<size).each{
            println entities[recommendType] + sortedlist[it].id + "  score " + sortedlist[it].rec
        }
    }
    println 'complete execution'
}


class recommendation {
   def id
   def rec
}

def getResults(Vertex v, String[] propertyList, String vectorValue, String biasOn) {
    def list = []
    length = propertyList.length
    valueLength = length
    if(length == 0){
      println "ERROR: no property provided for ML result!"
    } else if (vectorValue == "true"  &&
            biasOn == "true"  &&
            length != 2){
      println "ERROR: wrong property length provided for ML result!"
    }

    //firstly add bias
    if(biasOn == "true"){
      list.add v.getProperty(propertyList[length-1]).toDouble()
      valueLength = length - 1
    }

    //then add the results
    if(vectorValue == "true"){
      values = v.getProperty(propertyList[0]).split("[\\s,\\t]+")
        for(i in 0..<values.size()){
            list.add values[i].toDouble()
        }
    } else {
        for(i in 0..<valueLength){
            list.add v.getProperty(propertyList[i]).toDouble()
        }
    }

    return list
}

def calculateScore(list1, list2, biasOn, featureDimension) {
    if(biasOn == "true"){
        sum = list1[0] + list2[0]
        (1..featureDimension).each {
            sum += list1[it] * list2[it]
        }
    } else {
        (0..<featureDimension).each {
            sum = list1[it] * list2[it]
        }
    }

    return sum
}
       */
    }

    //TODO: We'll probably return an Iterable[Vertex] instead of rows at some point.
    override def getVertices(graph: Identifier,
                             offset: Int,
                             count: Int,
                             queryName: String,
                             parameters: Map[String, String]): Future[Iterable[Row]] = {
      //TODO: these will come from a dynamically configured map rather than a match expression
      future {
        queryName match {
          case "ALSQuery" => alsQuery(graph, offset, count, parameters)
          case _ => throw new IllegalArgumentException("Unknown query: " + queryName)
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

      val root: String = conf.getString("intel.analytics.fs.root")
      if (root.startsWith("hdfs"))
        hadoopConfig.set("fs.defaultFS", root)

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
          //case s if s.isDirectory => Directory(path = Paths.get(s.getPath.toString))
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

    def updateName(frame: DataFrame, newName: String): DataFrame = {
      val newFrame = frame.copy(name = newName)
      updateMeta(newFrame)
    }

    def updateSchema(frame: DataFrame, columns: List[(String, DataType)]): DataFrame = {
      val newSchema = frame.schema.copy(columns = columns)
      val newFrame = frame.copy(schema = newSchema)
      updateMeta(newFrame)
    }

    private def updateMeta(newFrame: DataFrame): DataFrame = {
      val meta = File(Paths.get(getFrameMetaDataFile(newFrame.id)))
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
        updateSchema(frame, remainingColumns)
      }

    override def addColumnWithValue[T](frame: DataFrame, column: Column[T], default: T): Unit =
      withContext("frame.addColumnWithValue") {
        ???
      }

    override def renameFrame(frame: DataFrame, newName: String): Unit =
      withContext("frame.rename") {
        updateName(frame, newName)
      }

    override def renameColumn(frame: DataFrame, name_pairs: Seq[(String, String)]): Unit =
      withContext("frame.renameColumn") {
        val columnsToRename: Seq[String] = name_pairs.map(_._1)
        val newColumnNames: Seq[String] = name_pairs.map(_._2)

        def generateNewColumnTuple(oldColumn: String, columnsToRename: Seq[String], newColumnNames: Seq[String]): String = {
          val result = columnsToRename.indexOf(oldColumn) match {
            case notFound if notFound < 0 => oldColumn
            case found => newColumnNames(found)
          }
          result
        }

        val newColumns = frame.schema.columns.map(col => (generateNewColumnTuple(col._1, columnsToRename, newColumnNames), col._2))
        updateSchema(frame, newColumns)
      }

    override def addColumn[T](frame: DataFrame, column: Column[T], columnType: DataTypes.DataType): DataFrame =
      withContext("frame.addColumn") {
        val newColumns = frame.schema.columns :+ (column.name, columnType)
        updateSchema(frame, newColumns)
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
      val frame2 = new DataFrame(id = id, name = frame.name)
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

  val graphs: GraphStorage =
    new SparkGraphStorage(engine.context(_),
      metaStore,
      new SparkGraphHBaseBackend(new HBaseAdmin(HBaseConfiguration.create())),
      frames)

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

    override def complete(id: Long, result: Try[JsObject]): Unit = {
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
            case Success(r) => command.copy(complete = true, result = Some(r))
          }
          repo.update(changed)
      }
    }

  }

}

