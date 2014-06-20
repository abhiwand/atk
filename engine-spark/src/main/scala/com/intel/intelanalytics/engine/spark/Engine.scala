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

import com.intel.event.EventContext
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine._
import scala.concurrent._
import spray.json.{JsNull, JsObject}
import com.intel.intelanalytics.engine.spark.frame.{SparkFrameStorage, RowParser, RDDJoinParam}
import scala.util.Try
import org.apache.spark.api.python.{EnginePythonAccumulatorParam, EnginePythonRDD}
import org.apache.spark.rdd.RDD
import scala.List
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import com.intel.intelanalytics.domain.schema.{SchemaUtil, DataTypes}
import DataTypes.DataType
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.domain.frame._
import java.util.{List => JList, ArrayList => JArrayList}
import spray.json._
import DomainJsonProtocol._
import com.intel.intelanalytics.domain.frame.FrameRenameFrame
import scala.Some
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.domain.frame.FrameAddColumns
import com.intel.intelanalytics.domain.frame.FrameRenameColumn
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.engine.spark.context.Context
import com.intel.intelanalytics.domain.graph.GraphLoad
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.domain.frame.LoadLines
import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.frame.FrameProject
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.domain.Partial
import com.intel.intelanalytics.domain.frame.SeparatorArgs
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.domain.frame.FlattenColumn
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.FrameRemoveColumn
import com.intel.intelanalytics.domain.frame.FrameJoin
import com.intel.intelanalytics.engine.spark.frame.RDDJoinParam
import com.intel.intelanalytics.domain.graph.GraphTemplate

//TODO: Fix execution contexts.
import ExecutionContext.Implicits.global

object SparkEngine {
  private val pythonRddDelimiter = "\0"
}

class SparkEngine(sparkContextManager: SparkContextManager,
                  commands: CommandStorage,
                  frames: SparkFrameStorage,
                  graphs: GraphStorage) extends Engine
                                        with EventLogging
                                        with ClassLoaderAware {

  val fsRoot = SparkEngineConfig.fsRoot


  def shutdown: Unit = {
    sparkContextManager.cleanup()
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
              val ctx = sparkContextManager.context(user)
              SparkOps.loadLines(ctx.sparkContext, fsRoot + "/" + arguments.source, location, arguments, parserFunction, converter)
              val frame = frames.updateSchema(realFrame, schema.columns)
              frame.toJson.asJsObject
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
              frames.renameFrame(frame, newName).toJson.asJsObject
            }
            commands.lookup(command.id).get
          }
        }
      }
      (command, result)
    }

  def renameColumn(arguments: FrameRenameColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
    withContext("se.rename_column") {
      require(arguments != null, "arguments are required")
      val command: Command = commands.create(new CommandTemplate("rename_column", Some(arguments.toJson.asJsObject)))
      val result: Future[Command] = future {
        withMyClassLoader {
          withContext("se.rename_column.future") {
            withCommand(command) {

              val frameID = arguments.frame

              val frame = frames.lookup(frameID).getOrElse(
                throw new IllegalArgumentException(s"No such data frame: $frameID"))

              frames.renameColumn(frame, arguments.original_names.zip(arguments.new_names)).toJson.asJsObject
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

              val ctx = sparkContextManager.context(user).sparkContext
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
              frames.updateSchema(projectedFrame, projectedColumns.toList).toJson.asJsObject
            }
            commands.lookup(command.id).get
          }
        }
      }
      (command, result)
    }

  def groupBy(arguments: FrameGroupByColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
    withContext("se.groupBy") {
      require(arguments != null, "arguments are required")
      val command: Command = commands.create(new CommandTemplate("groupBy", Some(arguments.toJson.asJsObject)))
      val result: Future[Command] = future {
        withMyClassLoader {
          withContext("se.groupBy.future") {
            withCommand(command) {

              val originalFrameID = arguments.frame

              val originalFrame = frames.lookup(originalFrameID).getOrElse(
                throw new IllegalArgumentException(s"No such data frame: $originalFrameID"))

              val ctx = sparkContextManager.context(user).sparkContext
              val schema = originalFrame.schema

              val newFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)

              val location = fsRoot + frames.getFrameDataFile(newFrame.id)

              val aggregation_arguments = arguments.aggregations

              val args_pair = for {
                (aggregation_function, column_to_apply, new_column_name) <- aggregation_arguments
              } yield (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_to_apply), aggregation_function)

              val new_data_types = if (arguments.group_by_columns.length > 0) {
                val groupByColumns = arguments.group_by_columns

                val columnIndices: Seq[(Int, DataType)] = for {
                  col <- groupByColumns
                  columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
                  columnDataType = schema.columns(columnIndex)._2
                } yield (columnIndex, columnDataType)

                val groupedRDD = frames.getFrameRdd(ctx, originalFrameID).groupBy((data: Rows.Row) => {
                  for { index <- columnIndices.map(_._1) } yield data(index)
                }.mkString("\0"))
                SparkOps.aggregation(groupedRDD, args_pair, originalFrame.schema.columns, columnIndices.map(_._2).toArray, location)
              }
              else {
                val groupedRDD = frames.getFrameRdd(ctx, originalFrameID).groupBy((data: Rows.Row) => "")
                SparkOps.aggregation(groupedRDD, args_pair, originalFrame.schema.columns, Array[DataType](), location)
              }
              val new_column_names = arguments.group_by_columns ++ { for {i <- aggregation_arguments} yield i._3 }
              val new_schema = new_column_names.zip(new_data_types)
              frames.updateSchema(newFrame, new_schema)
              newFrame.copy(schema = Schema(new_schema)).toJson.asJsObject
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
      val ctx = sparkContextManager.context(user).sparkContext
      val predicateInBytes = decodePythonBase64EncodedStrToBytes(py_expression)

      val baseRdd: RDD[String] = frames.getFrameRdd(ctx, frameId)
        .map(x => x.map(t => t.toString()).mkString(SparkEngine.pythonRddDelimiter))

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
      pyRdd.map(s => new String(s).split(SparkEngine.pythonRddDelimiter)).map(converter).saveAsObjectFile(location)
    }
  }

  /**
   * flatten rdd by the specified column
   * @param flattenColumnCommand input specification for column flattening
   * @param user current user
   */
  override def flattenColumn(flattenColumnCommand: FlattenColumn)(implicit user: UserPrincipal): (Command, Future[Command]) =
    withContext("se.flattenColumn") {
      val command: Command = commands.create(new CommandTemplate("flattenColumn", Some(flattenColumnCommand.toJson.asJsObject)))
      val result: Future[Command] = future {
        withMyClassLoader {
          withContext("se.flattenColumn.future") {
            val frameId: Long = flattenColumnCommand.frameId
            val realFrame = frames.lookup(frameId).getOrElse(
              throw new IllegalArgumentException(s"No such data frame: ${frameId}"))

            withCommand(command) {
              val ctx = sparkContextManager.context(user).sparkContext

              val newFrame = Await.result(create(DataFrameTemplate(flattenColumnCommand.name, None)), SparkEngineConfig.defaultTimeout)
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
              realFrame.toJson.asJsObject
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
            val columnIndex = frameSchema.columnIndex(frame._2)
            (rdd, columnIndex)
          }
        }

        val pairRdds = tupleRddColumnIndex.map {
          t =>
            val rdd = t._1
            val columnIndex = t._2
            rdd.map(p => SparkOps.createKeyValuePairFromRow(p, Seq(columnIndex))).map {case (keyColumns, data) => (keyColumns(0), data)}
        }

        pairRdds
      }

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
            val newJoinFrame = Await.result(create(DataFrameTemplate(joinCommand.name, None)), SparkEngineConfig.defaultTimeout)

            withCommand(command) {

              //first validate join columns are valid
              val leftOn: String = joinCommand.frames(0)._2
              val rightOn: String = joinCommand.frames(1)._2

              val leftSchema = Schema(leftColumns)
              val rightSchema = Schema(rightColumns)

              require(leftSchema.columnIndex(leftOn) != -1, s"column $leftOn is invalid")
              require(rightSchema.columnIndex(rightOn) != -1, s"column $rightOn is invalid")

              val ctx = sparkContextManager.context(user).sparkContext
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
      val command: Command = commands.create(new CommandTemplate("removecolumn", Some(arguments.toJson.asJsObject)))
      val result: Future[Command] = future {
        withMyClassLoader {
          withContext("se.removecolumn.future") {
            withCommand(command) {

              val ctx = sparkContextManager.context(user).sparkContext
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

              frames.removeColumn(realFrame, columnIndices).toJson.asJsObject
            }
            commands.lookup(command.id).get
          }
        }
      }
      (command, result)
    }

  def addColumns(arguments: FrameAddColumns[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command]) =
    withContext("se.add_columns") {
      require(arguments != null, "arguments are required")
      import DomainJsonProtocol._
      val command: Command = commands.create(new CommandTemplate("add_columns", Some(arguments.toJson.asJsObject)))
      val result: Future[Command] = future {
        withMyClassLoader {
          withContext("se.add_columns.future") {
            withCommand(command) {

              val ctx = sparkContextManager.context(user).sparkContext
              val frameId = arguments.frame
              val column_names = arguments.column_names
              val column_types = arguments.column_types
              val expression = arguments.expression // Python Wrapper containing lambda expression

              val realFrame = frames.lookup(arguments.frame).getOrElse(
                throw new IllegalArgumentException(s"No such data frame: ${arguments.frame}"))
              val schema = realFrame.schema
              val location = fsRoot + frames.getFrameDataFile(frameId)

              var newFrame = realFrame
              for {
                i <- 0 until column_names.size
              } {
                val column_name = column_names(i)
                val column_type = column_types(i)
                val columnObject = new BigColumn(column_name)

                if (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_name) >= 0)
                  throw new IllegalArgumentException(s"Duplicate column name: $column_name")

                // Update the schema
                newFrame = frames.addColumn(newFrame, columnObject, DataTypes.toDataType(column_type))
              }

              // Update the data
              val pyRdd = createPythonRDD(frameId, expression)
              val converter = DataTypes.parseMany(newFrame.schema.columns.map(_._2).toArray)(_)
              persistPythonRDD(pyRdd, converter, location)
              newFrame.toJson.asJsObject
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

  def getFrame(id: Identifier): Future[Option[DataFrame]] =
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
  def getGraph(id: Identifier): Future[Graph] = {
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


  //TODO: We'll probably return an Iterable[Vertex] instead of rows at some point.
  override def getVertices(graph: Identifier,
                           offset: Int,
                           count: Int,
                           queryName: String,
                           parameters: Map[String, String]): Future[Iterable[Row]] = {
   ???
  }

  override def dropDuplicates(dropDuplicateCommand: DropDuplicates)(implicit user: UserPrincipal): (Command, Future[Command]) =
    withContext("se.dropDuplicates") {
      require(dropDuplicateCommand != null, "arguments are required")

      import spray.json._
      val command: Command = commands.create(new CommandTemplate("dropDuplicates", Some(dropDuplicateCommand.toJson.asJsObject)))

      val result: Future[Command] = future {
        withMyClassLoader {
          withContext("se.dropDuplicates.future") {
            withCommand(command) {

              val frameId: Long = dropDuplicateCommand.frameId
              val realFrame: DataFrame = getDataFrameById(frameId)

              val ctx = sparkContextManager.context(user).sparkContext

              val frameSchema = realFrame.schema
              val rdd = frames.getFrameRdd(ctx, frameId)

              val columnIndices = frameSchema.columnIndex(dropDuplicateCommand.unique_columns)
              val pairRdd = rdd.map(row => SparkOps.createKeyValuePairFromRow(row, columnIndices))

              val duplicatesRemoved: RDD[Array[Any]] = SparkOps.removeDuplicatesByKey(pairRdd)

              duplicatesRemoved.saveAsObjectFile(fsRoot + frames.getFrameDataFile(frameId))
              JsObject(List())
            }
            commands.lookup(command.id).get
          }
        }
      }
      (command, result)
    }


  /**
   * Retrieve DataFrame object by frame id
   * @param frameId id of the dataframe
   */
  def getDataFrameById(frameId: Long): DataFrame = {
    val realFrame = frames.lookup(frameId).getOrElse(
      throw new IllegalArgumentException(s"No such data frame $frameId"))
    realFrame
  }
}
