//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine.plugin.Invocation
import spray.json._
import spray.http.{ StatusCodes, Uri }
import scala.Some
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.engine.{ Engine, EngineComponent }
import scala.concurrent._
import scala.util._
import com.intel.intelanalytics.service.v1.viewmodels.GetGraph
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphEntity }
import com.intel.intelanalytics.domain.DomainJsonProtocol.DataTypeFormat
import com.intel.intelanalytics.service.{ ApiServiceConfig, CommonDirectives, AuthenticationDirective }
import spray.routing.Directives
import com.intel.intelanalytics.service.v1.decorators.{ FrameDecorator, GraphDecorator }

import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits
import com.intel.intelanalytics.service.v1.viewmodels.Rel
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
import com.intel.event.EventLogging

//TODO: Is this right execution context for us?

import ExecutionContext.Implicits.global

/**
 * REST API Graph Service.
 *
 * Always use onComplete( Future { operationsGoHere() } ) to prevent "server disconnected" messages in client.
 */
class GraphService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * The spray routes defining the Graph service.
   */
  def graphRoutes() = {
    //import ViewModelJsonImplicits._
    val prefix = "graphs"
    val verticesPrefix = "vertices"
    val edgesPrefix = "edges"

    /**
     * Creates "decorated graph" for return on HTTP protocol
     * @param uri handle of graph
     * @param graph graph metadata
     * @return Decorated graph for HTTP protocol return
     */
    def decorate(uri: Uri, graph: GraphEntity): GetGraph = {
      //TODO: add other relevant links
      val links = List(Rel.self(uri.toString))
      GraphDecorator.decorateEntity(uri.toString, links, graph)
    }

    //TODO: none of these are yet asynchronous - they communicate with the engine
    //using futures, but they keep the client on the phone the whole time while they're waiting
    //for the engine work to complete. Needs to be updated to a) register running jobs in the metastore
    //so they can be queried, and b) support the web hooks.
    // note: delete is partly asynchronous - it wraps its backend delete in a future but blocks on deleting the graph
    // from the metastore

    commonDirectives(prefix) {
      implicit invocation: Invocation =>
        (path(prefix) & pathEnd) {
          requestUri {
            uri =>
              get {
                parameters('name.?) {
                  import spray.httpx.SprayJsonSupport._
                  implicit val indexFormat = ViewModelJsonImplicits.getGraphFormat
                  (name) => name match {
                    case Some(name) => {
                      onComplete(engine.getGraphByName(name)) {
                        case Success(Some(graph)) => {
                          val links = List(Rel.self(uri.toString))
                          complete(GraphDecorator.decorateEntity(uri.toString(), links, graph))
                        }
                        case Success(None) => complete(StatusCodes.NotFound, s"Graph with name '$name' was not found.")
                        case Failure(ex) => throw ex
                      }
                    }
                    case _ =>
                      //TODO: cursor
                      onComplete(engine.getGraphs()) {
                        case Success(graphs) =>
                          import IADefaultJsonProtocol._
                          implicit val indexFormat = ViewModelJsonImplicits.getGraphsFormat
                          complete(GraphDecorator.decorateForIndex(uri.toString(), graphs))
                        case Failure(ex) => throw ex
                      }
                  }
                }
              } ~
                post {
                  import spray.httpx.SprayJsonSupport._
                  implicit val format = DomainJsonProtocol.graphTemplateFormat
                  implicit val indexFormat = ViewModelJsonImplicits.getGraphFormat
                  entity(as[GraphTemplate]) {
                    graph =>
                      onComplete(engine.createGraph(graph)) {
                        case Success(graph) => complete(decorate(uri + "/" + graph.id, graph))
                        case Failure(ex) => ctx => {
                          ctx.complete(500, ex.getMessage)
                        }
                      }
                  }
                }
          }
        } ~
          pathPrefix(prefix / LongNumber) {
            id =>
              {
                pathEnd {
                  requestUri {
                    uri =>
                      get {
                        onComplete(engine.getGraph(id)) {
                          case Success(graph) => {
                            val decorated = decorate(uri, graph)
                            complete {
                              import spray.httpx.SprayJsonSupport._
                              implicit val format = DomainJsonProtocol.graphTemplateFormat
                              implicit val indexFormat = ViewModelJsonImplicits.getGraphFormat
                              decorated
                            }
                          }
                          case Failure(ex) => throw ex
                        }
                      } ~
                        delete {
                          onComplete(engine.deleteGraph(id)) {
                            case Success(ok) => complete("OK")
                            case Failure(ex) => throw ex
                          }
                        }
                  }
                } ~
                  pathPrefix(verticesPrefix) {
                    pathEnd {
                      requestUri {
                        uri =>
                          {
                            get {
                              import IADefaultJsonProtocol._
                              parameters('label.?) {
                                (label) =>
                                  label match {
                                    case Some(label) => {
                                      onComplete(engine.getVertex(id, label)) {
                                        case Success(Some(frame)) => {
                                          import spray.httpx.SprayJsonSupport._
                                          import ViewModelJsonImplicits.getDataFrameFormat
                                          complete(FrameDecorator.decorateEntity(uri.toString, Nil, frame))
                                        }
                                        case Failure(ex) => throw ex
                                      }
                                    }
                                    case None => {
                                      onComplete(engine.getVertices(id)) {
                                        case Success(frames) =>
                                          import spray.httpx.SprayJsonSupport._
                                          import IADefaultJsonProtocol._
                                          import ViewModelJsonImplicits.getDataFrameFormat
                                          complete(FrameDecorator.decorateEntities(uri.toString(), Nil, frames))
                                        case Failure(ex) => throw ex
                                      }
                                    }
                                  }
                              }
                            }
                          }
                      }
                    }
                  } ~
                  pathPrefix(edgesPrefix) {
                    pathEnd {
                      requestUri {
                        uri =>
                          {
                            get {
                              import IADefaultJsonProtocol._
                              parameters('label.?) {
                                (label) =>
                                  label match {
                                    case Some(label) => {
                                      onComplete(engine.getEdge(id, label)) {
                                        case Success(Some(frame)) => {
                                          import spray.httpx.SprayJsonSupport._
                                          import ViewModelJsonImplicits.getDataFrameFormat
                                          complete(FrameDecorator.decorateEntity(uri.toString, Nil, frame))
                                        }
                                        case Failure(ex) => throw ex
                                      }
                                    }
                                    case None => {
                                      onComplete(engine.getEdges(id)) {
                                        case Success(frames) =>
                                          import spray.httpx.SprayJsonSupport._
                                          import IADefaultJsonProtocol._
                                          import ViewModelJsonImplicits.getDataFrameFormat
                                          complete(FrameDecorator.decorateEntities(uri.toString(), Nil, frames))
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

              }
          }
    }
  }
}
