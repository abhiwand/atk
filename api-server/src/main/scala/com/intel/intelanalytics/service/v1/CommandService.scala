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

package com.intel.intelanalytics.service.v1

import com.intel.intelanalytics.domain.frame.load.{ LoadSource, Load }

import scala.util.Try
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits._
import scala.concurrent._
import spray.http.Uri
import spray.routing.{ ValidationRejection, Directives, Route }
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.domain.graph.construction.FrameRule
import scala.util.Failure
import scala.util.Success
import com.intel.intelanalytics.security.UserPrincipal
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits._
import com.intel.intelanalytics.domain.graph.{ GraphReference, GraphLoad }
import com.intel.intelanalytics.domain.command.{ Execution, CommandTemplate, Command }
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.service.{ ApiServiceConfig, UrlParser, CommonDirectives, AuthenticationDirective }
import com.intel.intelanalytics.service.v1.decorators.CommandDecorator
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol

//TODO: Is this right execution context for us?

import ExecutionContext.Implicits.global

/**
 * REST API Command Service
 */
class CommandService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * Creates a view model for return through the HTTP protocol
   *
   * @param uri The link representing the command.
   * @param command The command being decorated
   * @return View model of the command.
   */
  def decorate(uri: Uri, command: Command): GetCommand = {
    //TODO: add other relevant links
    val links = List(Rel.self(uri.toString()))
    CommandDecorator.decorateEntity(uri.toString(), links, command)
  }

  /**
   * The spray routes defining the command service.
   */
  def commandRoutes() = {
    commonDirectives("commands") { implicit principal: UserPrincipal =>
      pathPrefix("commands" / LongNumber) {
        id =>
          pathEnd {
            requestUri {
              uri =>
                get {
                  onComplete(engine.getCommand(id)) {
                    case Success(Some(command)) => complete(decorate(uri, command))
                    case _ => reject()
                  }
                }
            }
          }
      } ~
        pathPrefix("commands") {
          path("definitions") {
            get {
              import IADefaultJsonProtocol.listFormat
              import DomainJsonProtocol.commandDefinitionFormat
              complete(engine.getCommandDefinitions().toList)
            }
          } ~
            pathEnd {
              requestUri {
                uri =>
                  get {
                    //TODO: cursor
                    import spray.json._
                    import ViewModelJsonImplicits._
                    onComplete(engine.getCommands(0, ApiServiceConfig.defaultCount)) {
                      case Success(commands) => complete(CommandDecorator.decorateForIndex(uri.toString(), commands))
                      case Failure(ex) => throw ex
                    }
                  } ~
                    post {
                      entity(as[JsonTransform]) {
                        xform =>
                          try {
                            //TODO: this execution path is going away soon.
                            runCommand(uri, xform)
                          }
                          catch {
                            case e: IllegalArgumentException => {
                              //TODO: this will be the only execution path, soon.
                              val template = CommandTemplate(name = xform.name, arguments = xform.arguments)
                              info(s"Received command template for execution: $template")
                              try {
                                engine.execute(template) match {
                                  case Execution(command, futureResult) =>
                                    complete(decorate(uri + "/" + command.id, command))
                                }
                              }
                              catch {
                                case e: DeserializationException =>
                                  reject(ValidationRejection(
                                    s"Incorrectly formatted JSON found while parsing command '${xform.name}':" +
                                      s" ${e.getMessage}", Some(e)))
                              }
                            }
                          }
                      }
                    }
              }

            }
        }
    }
  }

  //TODO: disentangle the command dispatch from the routing
  //TODO: this method is going away soon.
  /**
   * Command dispatcher that translates from HTTP pathname to command invocation
   * @param uri Path of command.
   * @param xform Argument parser.
   * @param user IMPLICIT. The user running the command.
   * @return Spray Route for command invocation
   */
  def runCommand(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal): Route = {
    xform.name match {
      //TODO: genericize function resolution and invocation
      //case ("graph/load") => runGraphLoad(uri, xform)
      //case ("graph/ml/als") => runAls(uri, xform)
      //case ("dataframe/load") => runFrameLoad(uri, xform)
      case ("dataframe/filter") => runFilter(uri, xform)
      //case ("dataframe/removecolumn") => runFrameRemoveColumn(uri, xform)
      //case ("dataframe/rename_frame") => runFrameRenameFrame(uri, xform)
      case ("dataframe/add_columns") => runFrameAddColumns(uri, xform)
      case ("dataframe/project") => runFrameProject(uri, xform)
      case ("dataframe/rename_column") => runFrameRenameColumn(uri, xform)
      case ("dataframe/join") => runJoinFrames(uri, xform)
      case ("dataframe/flattenColumn") => runflattenColumn(uri, xform)
      case ("dataframe/groupby") => runFrameGroupByColumn(uri, xform)
      case ("dataframe/drop_duplicates") => runDropDuplicates(uri, xform)
      case ("dataframe/binColumn") => runBinColumn(uri, xform)
      case ("dataframe/classification_metric") => runClassificationMetric(uri, xform)
      case ("dataframe/confusion_matrix") => runConfusionMatrix(uri, xform)
      case ("dataframe/cumulative_dist") => runCumulativeDist(uri, xform)
      case s: String => illegalArg("Command name is not supported: " + s)
      case _ => illegalArg("Command name was NOT a string")
    }
  }

  /**
   * Load a dataset and append it to an existing dataframe.
   *
   * @param uri Command path
   * @param xform Json Object used for parsing the command sent from the client.
   * @param user current user
   * @return
   */
  def runFrameLoad(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      import DomainJsonProtocol._
      xform.arguments.get.convertTo[Load]
    }
    val idOpt = test.toOption.map(args => args.destination.id)
    (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
      & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
        val args = test.get
        val id = idOpt.get
        val exec = engine.load(Load(FrameReference(id), args.source.source_type match {
          case "dataframe" => {
            val dataID = UrlParser.getFrameId(args.source.uri)
            validate(dataID.isDefined, "Source is not a valid data frame URL")
            LoadSource(args.source.source_type, dataID.get.toString, args.source.parser)
          }
          case _ => args.source
        }))
        complete(decorate(uri + "/" + exec.start.id, exec.start))
      }
  }

  /**
   * Translates tabular data into graph form and load it into a graph database.
   * @param uri Command path.
   * @param transform Translates arguments from HTTP/JSon to internal case classes.
   * @param user The user
   * @return A Spray route.
   */
  def runGraphLoad(uri: Uri, transform: JsonTransform)(implicit user: UserPrincipal): Route = {
    val test = Try {
      transform.arguments.get.convertTo[GraphLoad]
    }

    val frameIDsOpt: Option[List[Long]] =
      test.toOption.map(args => args.frame_rules.map(frule => frule.frame.id))

    val graphIDOpt = test.toOption.map(args => args.graph.id)

    (validate(test.isSuccess, "Failed to parse graph load descriptor: " + getErrorMessage(test))
      & validate(graphIDOpt.isDefined, "Target graph is not a valid graph URL")) {

        validate(frameIDsOpt.isDefined, "Error parsing per-frame graph construction rules") {
          val args = test.get

          val graphID = graphIDOpt.get

          val graphLoad = GraphLoad(GraphReference(graphID),
            args.frame_rules,
            args.append)
          val exec = engine.loadGraph(graphLoad)
          complete(decorate(uri + "/" + exec.start.id, exec.start))
        }
      }
  }

  def runFilter(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[FilterPredicate[JsObject, String]]
    }
    val idOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.frame))
    (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
      & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
        val args = test.get
        val id = idOpt.get
        val exec = engine.filter(FilterPredicate[JsObject, Long](id, args.predicate))
        complete(decorate(uri + "/" + exec.start.id, exec.start))
      }
  }

  def runFrameRenameColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[FrameRenameColumn[JsObject, String]]
    }
    val idOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.frame))
    (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
      & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
        val args = test.get
        val id = idOpt.get
        val exec = engine.renameColumn(FrameRenameColumn[JsObject, Long](id, args.original_names, args.new_names))
        complete(decorate(uri + "/" + exec.start.id, exec.start))
      }
  }

  def runFrameAddColumns(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[FrameAddColumns[JsObject, String]]
    }
    val idOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.frame))
    (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
      & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
        val args = test.get
        val id = idOpt.get
        val exec = engine.addColumns(FrameAddColumns[JsObject, Long](id, args.column_names, args.column_types, args.expression))
        complete(decorate(uri + "/" + exec.start.id, exec.start))
      }
  }

  /**
   * Perform the join operation and return the submitted command to the client
   */
  def runJoinFrames(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal): Route = {
    val test = Try {
      xform.arguments.get.convertTo[FrameJoin]
    }

    validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test)) {
      val args = test.get
      val result = engine.join(args)
      val command: Command = result.start
      complete(decorate(uri + "/" + command.id, command))
    }

  }

  /**
   * Receive column flattening request and executing flatten column command
   */
  def runflattenColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[FlattenColumn]
    }

    validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test)) {
      val args = test.get
      val result = engine.flattenColumn(args)
      val command: Command = result.start
      complete(decorate(uri + "/" + command.id, command))
    }
  }

  def runBinColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[BinColumn[Long]]
    }

    validate(test.isSuccess, "Failed to parse bin column descriptor: " + getErrorMessage(test)) {
      val args = test.get
      val result = engine.binColumn(args)
      val command: Command = result.start
      complete(decorate(uri + "/" + command.id, command))
    }
  }

  /**
   * Receive drop duplicates request and executing drop duplicates
   */
  def runDropDuplicates(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[DropDuplicates]
    }

    validate(test.isSuccess, "Failed to parse drop duplicates descriptor: " + getErrorMessage(test)) {
      val args = test.get
      val result = engine.dropDuplicates(args)
      val command: Command = result.start
      complete(decorate(uri + "/" + command.id, command))
    }
  }

  def runFrameProject(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[FrameProject[JsObject, String]]
    }
    val sourceFrameIdOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.frame))
    val projectedFrameIdOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.projected_frame))
    (validate(test.isSuccess, "Failed to project command descriptor: " + getErrorMessage(test))
      & validate(projectedFrameIdOpt.isDefined, "Destination is not a valid data frame")) {
        val args = test.get
        val sourceFrameId = sourceFrameIdOpt.get
        val projectedFrameId = projectedFrameIdOpt.get
        val exec = engine.project(FrameProject[JsObject, Long](sourceFrameId, projectedFrameId, args.columns, args.new_column_names))
        complete(decorate(uri + "/" + exec.start.id, exec.start))
      }
  }

  def runFrameGroupByColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        import DomainJsonProtocol._
        xform.arguments.get.convertTo[FrameGroupByColumn[JsObject, String]]
      }
      val idOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.frame))
      (validate(test.isSuccess, "Failed to : " + getErrorMessage(test))
        & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
          val args = test.get
          val id = idOpt.get
          val exec = engine.groupBy(FrameGroupByColumn[JsObject, Long](id, args.name, args.group_by_columns, args.aggregations))
          complete(decorate(uri + "/" + exec.start.id, exec.start))
        }
    }
  }

  def runClassificationMetric(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        xform.arguments.get.convertTo[ClassificationMetric[Long]]
      }

      validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test)) {
        val args = test.get
        val result = engine.classificationMetric(args)
        val command: Command = result.start
        complete(decorate(uri + "/" + command.id, command))
      }
    }
  }

  def runConfusionMatrix(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        xform.arguments.get.convertTo[ConfusionMatrix[Long]]
      }

      validate(test.isSuccess, "Failed to parse confusion matrix descriptor: " + getErrorMessage(test)) {
        val args = test.get
        val result = engine.confusionMatrix(args)
        val command: Command = result.start
        complete(decorate(uri + "/" + command.id, command))
      }
    }
  }

  def runCumulativeDist(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    {
      val test = Try {
        xform.arguments.get.convertTo[CumulativeDist[Long]]
      }

      validate(test.isSuccess, "Failed to parse cumulative distribution descriptor: " + getErrorMessage(test)) {
        val args = test.get
        val result = engine.cumulativeDist(args)
        val command: Command = result.start
        complete(decorate(uri + "/" + command.id, command))
      }
    }
  }

  //TODO: internationalization
  def getErrorMessage[T](value: Try[T]): String = value match {
    case Success(x) => ""
    case Failure(ex) => ex.getMessage
  }
}
