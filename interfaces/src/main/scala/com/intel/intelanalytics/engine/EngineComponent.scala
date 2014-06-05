package com.intel.intelanalytics.engine

trait EngineComponent {

  import Rows.Row

  type Identifier = Long //TODO: make more generic?

  def engine: Engine

  //TODO: make these all use Try instead?
  //TODO: make as many of these as possible use id instead of dataframe as the first argument?
  //TODO: distinguish between DataFrame and DataFrameSpec,
  // where the latter has no ID, and is the argument passed to create?
  trait Engine {
    //TODO: We'll probably return an Iterable[Vertex] instead of rows at some point.
    def getVertices(graph: Identifier, offset: Int, count: Int, queryName: String, parameters: Map[String, String]): Future[Iterable[Row]]

    def getCommands(offset: Int, count: Int): Future[Seq[Command]]

    def getCommand(id: Identifier): Future[Option[Command]]

    def getFrame(id: Identifier): Future[Option[DataFrame]]

    def getRows(id: Identifier, offset: Long, count: Int)(implicit user: UserPrincipal): Future[Iterable[Row]]

    def create(frame: DataFrameTemplate): Future[DataFrame]

    def clear(frame: DataFrame): Future[DataFrame]

    def load(arguments: LoadLines[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def filter(arguments: FilterPredicate[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def project(arguments: FrameProject[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def renameFrame(arguments: FrameRenameFrame[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def renameColumn(arguments: FrameRenameColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

    //  Should predicate be Partial[Any]  def filter(frame: DataFrame, predicate: Partial[Any])(implicit user: UserPrincipal): Future[DataFrame]
    def removeColumn(arguments: FrameRemoveColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def addColumn(arguments: FrameAddColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def alter(frame: DataFrame, changes: Seq[Alteration])

    def delete(frame: DataFrame): Future[Unit]
    def join(argument: FrameJoin)(implicit user: UserPrincipal): (Command, Future[Command])
    def flattenColumn(argument: FlattenColumn[Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def getFrames(offset: Int, count: Int)(implicit p: UserPrincipal): Future[Seq[DataFrame]]

    def shutdown: Unit

    def getGraph(id: Identifier): Future[Graph]

    def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal): Future[Seq[Graph]]

    def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Future[Graph]

    def loadGraph(graph: GraphLoad[JsObject, Long, Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def deleteGraph(graph: Graph): Future[Unit]

    //NOTE: we do /not/ expect to have a separate method for every single algorithm, this will move to a plugin
    //system soon
    def runAls(als: Als[Long]): (Command, Future[Command])
  }

}
