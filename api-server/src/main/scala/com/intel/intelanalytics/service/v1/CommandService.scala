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

import scala.util.Try
import com.intel.intelanalytics.domain._
import spray.json.JsObject
import com.intel.intelanalytics.repository.MetaStoreComponent
import com.intel.intelanalytics.engine.{Engine, EngineComponent}
import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits._
import scala.concurrent._
import spray.http.Uri
import spray.routing.{Directives, Route}
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.domain.graph.construction.FrameRule
import scala.util.Failure
import scala.Some
import scala.util.Success
import com.intel.intelanalytics.security.UserPrincipal
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits._
import com.intel.intelanalytics.domain.graph.GraphLoad
import com.intel.intelanalytics.domain.command.{CommandTemplate, Command}
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.service.{ApiServiceConfig, UrlParser, CommonDirectives, AuthenticationDirective}
import com.intel.intelanalytics.service.v1.decorators.CommandDecorator

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
        (path("commands") & pathEnd) {
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
                          //TODO: validate the arguments. To do this requires some kind of sharing
                          //between the api server and the engine to determine what contracts to use.
                          //TODO: standardize URI handling such that the API server strips out
                          //the https://site.com/ part and leaves the engine with only an application-specific,
                          //non-transport-related URI. This should be automatic and not something that
                          //every command handler has to call.
                          onComplete(engine.execute(CommandTemplate(name = xform.name, arguments = xform.arguments))) {
                            case Success((command, futureResult)) =>
                              complete(decorate(uri, command))
                            case Failure(ex) => throw ex
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
      case ("graph/load") => runGraphLoad(uri, xform)
      //case ("graph/ml/als") => runAls(uri, xform)
      case ("dataframe/load") => runFrameLoad(uri, xform)
      case ("dataframe/filter") => runFilter(uri, xform)
      case ("dataframe/removecolumn") => runFrameRemoveColumn(uri, xform)
      case ("dataframe/rename_frame") => runFrameRenameFrame(uri, xform)
      case ("dataframe/add_columns") => runFrameAddColumns(uri, xform)
      case ("dataframe/project") => runFrameProject(uri, xform)
      case ("dataframe/rename_column") => runFrameRenameColumn(uri, xform)
      case ("dataframe/join") => runJoinFrames(uri, xform)
      case ("dataframe/flattenColumn") => runflattenColumn(uri, xform)
      case ("dataframe/groupby") => runFrameGroupByColumn(uri, xform)
      case ("dataframe/binColumn") => runBinColumn(uri, xform)
      case s: String => illegalArg("Command name is not supported: " + s)
      case _ => illegalArg("Command name was NOT a string")
    }
  }

  def runFrameLoad(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      import DomainJsonProtocol._
      xform.arguments.get.convertTo[LoadLines[JsObject, String]]
    }
    val idOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.destination))
    (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
      & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
      val args = test.get
      val id = idOpt.get
      onComplete(
        for {
          frame <- engine.getFrame(id)
          (c, f) = engine.load(LoadLines[JsObject, Long](args.source, id,
            skipRows = args.skipRows, overwrite = args.overwrite, lineParser = args.lineParser, schema = args.schema))
        } yield c) {
        case Success(c) => complete(decorate(uri + "/" + c.id, c))
        case Failure(ex) => throw ex
      }
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
      transform.arguments.get.convertTo[GraphLoad[JsObject, String, String]]
    }

    val frameIDsOpt: Option[List[Option[Long]]] =
      test.toOption.map(args => args.frame_rules.map(frule => UrlParser.getFrameId(frule.frame)))

    val graphIDOpt = test.toOption.flatMap(args => UrlParser.getGraphId(args.graph))

    (validate(test.isSuccess, "Failed to parse graph load descriptor: " + getErrorMessage(test))
      & validate(graphIDOpt.isDefined, "Target graph is not a valid graph URL")) {

      (validate(frameIDsOpt.isDefined, "Error parsing per-frame graph construction rules")
        & validate(frameIDsOpt.get.forall(x => x.isDefined), "Invalid URL provided for source dataframe")) {
        val args = test.get
        val sourceFrameIDs = frameIDsOpt.get.map(x => x.get)

        val frameRulesUsingIDs = (sourceFrameIDs, args.frame_rules).zipped.toList.map { case (id: Long, frule: FrameRule[String]) => new FrameRule[Long](id, frule.vertex_rules, frule.edge_rules)}

        val graphID = graphIDOpt.get

        val graphLoad = GraphLoad(graphID,
          frameRulesUsingIDs,
          args.retain_dangling_edges)

        onComplete(
          for {
            graph <- engine.getGraph(graphID)
            (c, f) = engine.loadGraph(graphLoad)
          } yield c ) {
          case Success(c) => complete(decorate(uri + "/" + c.id, c))
          case Failure(ex) => throw ex
        }
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
      onComplete(
        for {
          frame <- engine.getFrame(id)
          (c, f) = engine.filter(FilterPredicate[JsObject, Long](id, args.predicate))
        } yield c) {
        case Success(c) => complete(decorate(uri + "/" + c.id, c))
        case Failure(ex) => throw ex
      }
    }
  }

  def runFrameRenameFrame(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[FrameRenameFrame[JsObject, String]]
    }
    val idOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.frame))
    (validate(test.isSuccess, "Failed to understand rename arguments: " + getErrorMessage(test))
      & validate(idOpt.isDefined, "Frame must be a valid data frame URL")) {
      val args = test.get
      val id = idOpt.get
      onComplete(
        for {
          frame <- engine.getFrame(id)
          (c, f) = engine.renameFrame(FrameRenameFrame[JsObject, Long](id, args.new_name))
        } yield c) {
        case Success(c) => complete(decorate(uri + "/" + c.id, c))
        case Failure(ex) => throw ex
      }
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
      onComplete(
        for {
          frame <- engine.getFrame(id)
          (c, f) = engine.renameColumn(FrameRenameColumn[JsObject, Long](id, args.original_names, args.new_names))
        } yield c) {
        case Success(c) => complete(decorate(uri + "/" + c.id, c))
        case Failure(ex) => throw ex
      }
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
      onComplete(
        for {
          frame <- engine.getFrame(id)
          (c, f) = engine.addColumns(FrameAddColumns[JsObject, Long](id, args.column_names, args.column_types, args.expression))
        } yield c) {
        case Success(c) => complete(decorate(uri + "/" + c.id, c))
        case Failure(ex) => throw ex
      }
    }
  }

  def runFrameRemoveColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[FrameRemoveColumn[JsObject, String]]
    }
    val idOpt = test.toOption.flatMap(args => UrlParser.getFrameId(args.frame))
    (validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test))
      & validate(idOpt.isDefined, "Destination is not a valid data frame URL")) {
      val args = test.get
      val id = idOpt.get
      onComplete(
        for {
          frame <- engine.getFrame(id)
          (c, f) = engine.removeColumn(FrameRemoveColumn[JsObject, Long](id, args.column))
        } yield c) {
        case Success(c) => complete(decorate(uri + "/" + c.id, c))
        case Failure(ex) => throw ex
      }
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
      val command: Command = result._1
      complete(decorate(uri + "/" + command.id, command))
    }

  }

  /**
   * Receive column flattening request and executing flatten column command
   */
  def runflattenColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[FlattenColumn[Long]]
    }

    validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test)) {
      val args = test.get
      val result = engine.flattenColumn(args)
      val command: Command = result._1
      complete(decorate(uri + "/" + command.id, command))
    }
  }

  def runBinColumn(uri: Uri, xform: JsonTransform)(implicit user: UserPrincipal) = {
    val test = Try {
      xform.arguments.get.convertTo[BinColumn[Long]]
    }

    validate(test.isSuccess, "Failed to parse file load descriptor: " + getErrorMessage(test)) {
      val args = test.get
      val result = engine.binColumn(args)
      val command: Command = result._1
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
      onComplete(
        for {
          projectFrame <- engine.getFrame(projectedFrameId)
          sourceFrame <- engine.getFrame(sourceFrameId)
          (c, f) = engine.project(FrameProject[JsObject, Long](sourceFrameId, projectedFrameId, args.columns, args.new_column_names))
        } yield c) {
        case Success(c) => complete(decorate(uri + "/" + c.id, c))
        case Failure(ex) => throw ex
      }
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
        onComplete(
          for {
            frame <- engine.getFrame(id)
            (c, f) = engine.groupBy(FrameGroupByColumn[JsObject, Long](id, args.name, args.group_by_columns, args.aggregations))
          } yield c) {
          case Success(c) => complete(decorate(uri + "/" + c.id, c))
          case Failure(ex) => throw ex
        }
      }
    }
  }

  //TODO: internationalization
  def getErrorMessage[T](value: Try[T]): String = value match {
    case Success(x) => ""
    case Failure(ex) => ex.getMessage
  }
}
