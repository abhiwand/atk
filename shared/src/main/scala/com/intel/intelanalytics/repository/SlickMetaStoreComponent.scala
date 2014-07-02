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

package com.intel.intelanalytics.repository

import com.github.tototoshi.slick.GenericJodaSupport
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import com.intel.intelanalytics.domain.frame.{ DataFrame, DataFrameTemplate }
import com.intel.intelanalytics.domain.graph.{ Graph, GraphTemplate }
import com.intel.intelanalytics.domain.query.{QueryTemplate, Query => gaoQuery}
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.shared.EventLogging
import org.joda.time.DateTime
import scala.slick.driver.{ JdbcDriver, JdbcProfile }
import org.flywaydb.core.Flyway
import spray.json._

import scala.util.Try

trait SlickMetaStoreComponent extends MetaStoreComponent with EventLogging {
  msc: MetaStoreComponent with DbProfileComponent =>

  import com.intel.intelanalytics.domain.DomainJsonProtocol._
  import profile.profile.simple._

  // Joda Support depends on the driver being used.
  val genericJodaSupport = new GenericJodaSupport(profile.profile.asInstanceOf[JdbcDriver])
  // Different versions of implicits are imported here based on the driver
  import genericJodaSupport._

  // Defining mappings for custom column types
  implicit val schemaColumnType = MappedColumnType.base[Schema, String](
    { schema => schema.toJson.prettyPrint }, // Schema to String
    { string => JsonParser(string).convertTo[Schema] } // String to Schema
  )

  implicit val jsObjectColumnType = MappedColumnType.base[JsObject, String](
    { jsObject => jsObject.toString() },
    { string => JsonParser(string).convertTo[JsObject] }
  )

  implicit val errorColumnType = MappedColumnType.base[Error, String](
    { error => error.toJson.prettyPrint },
    { string => JsonParser(string).convertTo[Error] }
  )

  implicit val commandProgressType = MappedColumnType.base[List[Float], String](
    { progress => progress.toJson.prettyPrint },
    { string => JsonParser(string).convertTo[List[Float]] }
  )

  private[repository] val database = withContext("Connecting to database") {
    Database.forURL(profile.connectionString, driver = profile.driver, user = profile.username, password = profile.password)
  }

  type Session = profile.profile.simple.Session

  override lazy val metaStore: MetaStore = new SlickMetaStore

  /**
   * Implementation of MetaStore using Slick.
   *
   * The MetaStore is how you get access to Repositories.
   *
   * Repositories are how you modify and query underlying tables.
   */
  class SlickMetaStore extends MetaStore with EventLogging {
    type Session = msc.Session

    /**
     * Create the underlying tables, sequences, etc.
     */
    override def initializeSchema(): Unit = {

      withSession("Creating tables") {
        implicit session =>
          if (profile.isH2) {
            info("Creating schema using Slick")
            // Tables that are dependencies for other tables need to go first
            statusRepo.asInstanceOf[SlickStatusRepository].createTable
            statusRepo.asInstanceOf[SlickStatusRepository].initializeValues
            userRepo.asInstanceOf[SlickUserRepository].createTable
            frameRepo.asInstanceOf[SlickFrameRepository].createTable // depends on user, status
            commandRepo.asInstanceOf[SlickQueryRepository].createTable // depends on user
            queryRepo.asInstanceOf[SlickQueryRepository].createTable // depends on user
            graphRepo.asInstanceOf[SlickGraphRepository].createTable // depends on user, status
            info("Schema creation completed")
          }
          else {
            info("Running migrations to create/update schema as needed, jdbcUrl: " + profile.connectionString + ", user: " + profile.username)
            val flyway = new Flyway()
            flyway.setDataSource(profile.connectionString, profile.username, profile.password)
            flyway.migrate()
            info("Migration completed")
          }
      }
    }

    /** Delete ALL of the underlying tables - useful for unit tests only */
    private[repository] override def dropAllTables(): Unit = {

      withSession("Dropping all tables") {
        implicit session =>
          if (profile.isH2) {
            info("dropping")
            // Tables that are dependencies for other tables need to go last
            frameRepo.asInstanceOf[SlickFrameRepository].dropTable
            commandRepo.asInstanceOf[SlickCommandRepository].dropTable
            queryRepo.asInstanceOf[SlickQueryRepository].dropTable
            graphRepo.asInstanceOf[SlickGraphRepository].dropTable
            userRepo.asInstanceOf[SlickUserRepository].dropTable
            statusRepo.asInstanceOf[SlickStatusRepository].dropTable
            info("tables dropped")
          }
          else {
            throw new RuntimeException("Dropping tables is only supported for H2")
          }
      }
    }

    override lazy val statusRepo: Repository[Session, Status, Status] = new SlickStatusRepository

    override lazy val graphRepo: Repository[Session, GraphTemplate, Graph] = new SlickGraphRepository

    override lazy val frameRepo: Repository[Session, DataFrameTemplate, DataFrame] = new SlickFrameRepository

    /** Repository for CRUD on 'command' table */
    override lazy val commandRepo: CommandRepository[Session] = new SlickCommandRepository

    /** Repository for CRUD on 'command' table */
    override lazy val queryRepo: QueryRepository[Session] = new SlickQueryRepository

    /** Repository for CRUD on 'user' table */
    override lazy val userRepo: Repository[Session, UserTemplate, User] with Queryable[Session, User] = new SlickUserRepository

    override def withSession[T](name: String)(f: (Session) => T): T = {
      withContext(name) {
        database.withSession(f)
      }
    }
  }

  /**
   * A slick implementation of the 'User' table that defines
   * the columns and conversion to/from Scala beans.
   */
  class UserTable(tag: Tag) extends Table[User](tag, "users") {

    def id = column[Long]("user_id", O.PrimaryKey, O.AutoInc)

    def username = column[Option[String]]("username")

    def api_key = column[Option[String]]("api_key")

    def createdOn = column[DateTime]("created_on")

    def modifiedOn = column[DateTime]("modified_on")

    def * = (id, username, api_key, createdOn, modifiedOn) <> (User.tupled, User.unapply)
  }

  val users = TableQuery[UserTable]

  /**
   * A slick implementation of a User Repository.
   *
   * Provides methods for modifying and querying the user table.
   */
  class SlickUserRepository extends Repository[Session, UserTemplate, User]
      with Queryable[Session, User]
      with EventLogging {
    this: Repository[Session, UserTemplate, User] with Queryable[Session, User] =>

    protected val usersAutoInc = users returning users.map(_.id) into {
      case (c, id) => c.copy(id = id)
    }

    override def insert(newUser: UserTemplate)(implicit session: Session): Try[User] = Try {
      usersAutoInc.insert(User(0, None, Some(newUser.api_key), new DateTime(), new DateTime()))(session)
    }

    override def lookup(id: Long)(implicit session: Session): Option[User] = {
      users.where(_.id === id).firstOption
    }

    override def lookupByName(name: String)(implicit session: Session): Option[User] = {
      users.where(_.username === name).firstOption
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      users.where(_.id === id).mutate(c => c.delete())
    }

    override def update(user: User)(implicit session: Session): Try[User] = Try {
      // TODO: updated modifiedOn timestamp on all tables
      val updatedUser = user.copy(modifiedOn = new DateTime)
      users.where(_.id === user.id).update(updatedUser)
      updatedUser
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[User] = {
      users.drop(offset).take(count).list
    }

    override def retrieveByColumnValue(colName: String, value: String)(implicit session: Session): List[User] = {
      users.filter(_.column[Option[String]](colName) === value).list
    }

    /** execute DDL to create the underlying table */
    def createTable()(implicit session: Session) = {
      users.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      users.ddl.drop
    }

  }

  class StatusTable(tag: Tag) extends Table[Status](tag, "status") {
    def id = column[Long]("status_id", O.PrimaryKey)

    def name = column[String]("name")

    def description = column[String]("description")

    def createdOn = column[DateTime]("created_on")

    def modifiedOn = column[DateTime]("modified_on")

    /** projection to/from the database */
    def * = (id, name, description, createdOn, modifiedOn) <> (Status.tupled, Status.unapply)
  }

  val statuses = TableQuery[StatusTable]

  /**
   * A slick implementation of the graph repository. It stores metadata for statuses.
   */
  class SlickStatusRepository extends Repository[Session, Status, Status]
      with EventLogging {
    this: Repository[Session, Status, Status] =>

    override def insert(status: Status)(implicit session: Session): Try[Status] = {
      statuses.insert(status)
      Try(status)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      statuses.where(_.id === id).mutate(f => f.delete())
    }

    override def update(status: Status)(implicit session: Session): Try[Status] = Try {
      val updatedStatus = status.copy(modifiedOn = new DateTime)
      statuses.where(_.id === status.id).update(updatedStatus)
      updatedStatus
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[Status] = {
      statuses.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[Status] = {
      statuses.where(_.id === id).firstOption
    }

    override def lookupByName(name: String)(implicit session: Session): Option[Status] = {
      statuses.where(_.name === name).firstOption
    }

    def lookupInit()(implicit session: Session): Status = {
      lookup(1).get
    }

    def lookupActive()(implicit session: Session): Status = {
      lookup(2).get
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      statuses.ddl.create
    }

    /** Initialize this reference table with all possible values */
    def initializeValues(implicit session: Session) = {
      insert(Status(1, "INIT", "Initial Status: currently building or initializing", new DateTime(), new DateTime()))
      insert(Status(2, "ACTIVE", "Active and can be interacted with", new DateTime(), new DateTime()))
      insert(Status(3, "INCOMPLETE", "Partially created: failure occurred during construction.", new DateTime(), new DateTime()))
      insert(Status(4, "DELETED", "Deleted but can still be un-deleted, no action has yet been taken on disk", new DateTime(), new DateTime()))
      insert(Status(5, "DELETE_FINAL", "Underlying storage has been reclaimed, no un-delete is possible", new DateTime(), new DateTime()))
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      statuses.ddl.drop
    }
  }

  /**
   * A slick implementation of a Frame Repository.
   *
   * Provides methods for modifying and querying the frame table.
   */
  class SlickFrameRepository extends Repository[Session, DataFrameTemplate, DataFrame]
      with EventLogging {
    this: Repository[Session, DataFrameTemplate, DataFrame] =>

    /**
     * A slick implementation of the 'Frame' table that defines
     * the columns and conversion to/from Scala beans.
     */
    class FrameTable(tag: Tag) extends Table[DataFrame](tag, "frame") {
      def id = column[Long]("frame_id", O.PrimaryKey, O.AutoInc)

      def name = column[String]("name")

      def description = column[Option[String]]("description")

      def schema = column[Schema]("schema")

      def statusId = column[Long]("status_id", O.Default(1))

      def createdOn = column[DateTime]("created_on")

      def modifiedOn = column[DateTime]("modified_on")

      def createdById = column[Option[Long]]("created_by")

      def modifiedById = column[Option[Long]]("modified_by")

      /** projection to/from the database */
      override def * = (id, name, description, schema, statusId, createdOn, modifiedOn, createdById, modifiedById) <>
        (DataFrame.tupled, DataFrame.unapply)

      // foreign key relationships

      def status = foreignKey("frame_status_id", statusId, statuses)(_.id)

      def createdBy = foreignKey("frame_created_by", createdById, users)(_.id)

      def modifiedBy = foreignKey("frame_modified_by", modifiedById, users)(_.id)

    }

    val frames = TableQuery[FrameTable]

    protected val framesAutoInc = frames returning frames.map(_.id) into {
      case (f, id) => f.copy(id = id)
    }

    def _insertFrame(frame: DataFrameTemplate)(implicit session: Session) = {
      val f = DataFrame(0, frame.name, frame.description, Schema(), 1L, new DateTime(), new DateTime(), None, None)
      framesAutoInc.insert(f)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      frames.where(_.id === id).mutate(f => f.delete())
    }

    override def update(frame: DataFrame)(implicit session: Session): Try[DataFrame] = Try {
      val updatedFrame = frame.copy(modifiedOn = new DateTime)
      frames.where(_.id === frame.id).update(updatedFrame)
      updatedFrame
    }

    override def insert(frame: DataFrameTemplate)(implicit session: Session): Try[DataFrame] = Try {
      _insertFrame(frame)(session)
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[DataFrame] = {
      frames.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[DataFrame] = {
      frames.where(_.id === id).firstOption
    }
    override def lookupByName(name: String)(implicit session: Session): Option[DataFrame] = {
      frames.where(_.name === name).firstOption
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      frames.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      frames.ddl.drop
    }

  }

  /**
   * A slick implementation of a Command Repository.
   *
   * Provides methods for modifying and querying the command table.
   */
  class SlickCommandRepository extends CommandRepository[Session]
      with EventLogging {
    this: Repository[Session, CommandTemplate, Command] =>

    /**
     * A slick implementation of the 'Command' table that defines
     * the columns and conversion to/from Scala beans.
     */
    class CommandTable(tag: Tag) extends Table[Command](tag, "command") {
      def id = column[Long]("command_id", O.PrimaryKey, O.AutoInc)

      def name = column[String]("name")

      def arguments = column[Option[JsObject]]("arguments")

      def error = column[Option[Error]]("error")

      def progress = column[List[Float]]("progress")

      def complete = column[Boolean]("complete", O.Default(false))

      def result = column[Option[JsObject]]("result")

      def createdOn = column[DateTime]("created_on")

      def modifiedOn = column[DateTime]("modified_on")

      def createdById = column[Option[Long]]("created_by")

      /** projection to/from the database */
      def * = (id, name, arguments, error, progress, complete, result, createdOn, modifiedOn, createdById) <> (Command.tupled, Command.unapply)

      def createdBy = foreignKey("command_created_by", createdById, users)(_.id)
    }

    val commands = TableQuery[CommandTable]

    protected val commandsAutoInc = commands returning commands.map(_.id) into {
      case (f, id) => f.copy(id = id)
    }

    override def insert(command: CommandTemplate)(implicit session: Session): Try[Command] = Try {
      // TODO: add createdBy user id
      val c = Command(0, command.name, command.arguments, None, List(), false, None, new DateTime(), new DateTime(), None)
      commandsAutoInc.insert(c)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      commands.where(_.id === id).mutate(f => f.delete())
    }

    override def update(command: Command)(implicit session: Session): Try[Command] = Try {
      val updatedCommand = command.copy(modifiedOn = new DateTime())
      val updated = commands.where(_.id === command.id).update(updatedCommand)
      updatedCommand
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[Command] = {
      commands.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[Command] = {
      commands.where(_.id === id).firstOption
    }

    override def lookupByName(name: String)(implicit session: Session): Option[Command] = {
      commands.where(_.name === name).firstOption
    }

    /**
     * update the command to complete
     * @param id command id
     * @param complete the complete flag
     * @param session session to db
     */
    override def updateComplete(id: Long, complete: Boolean)(implicit session: Session): Try[Unit] = Try {
      val completeCol = for (c <- commands if c.id === id) yield c.complete
      completeCol.update(complete)
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      commands.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      commands.ddl.drop
    }

    /**
     * update the progress for the command
     * @param id command id
     * @param progress progress for the command
     * @param session session to db
     */
    override def updateProgress(id: Long, progress: List[Float])(implicit session: Session): Try[Unit] = Try {
      val q = for { c <- commands if c.id === id } yield c.progress
      q.update(progress)
    }
  }

  /**
   * A slick implementation of a Query Repository.
   *
   * Provides methods for modifying and querying the query table.
   */
  class SlickQueryRepository extends QueryRepository[Session]
  with EventLogging {
    this: Repository[Session, QueryTemplate, gaoQuery] =>

    /**
     * A slick implementation of the 'Query' table that defines
     * the columns and conversion to/from Scala beans.
     */
    class QueryTable(tag: Tag) extends Table[gaoQuery](tag, "command") {
      def id = column[Long]("query_id", O.PrimaryKey, O.AutoInc)

      def name = column[String]("name")

      def arguments = column[Option[JsObject]]("arguments")

      def error = column[Option[Error]]("error")

      def progress = column[List[Float]]("progress")

      def complete = column[Boolean]("complete", O.Default(false))

      def result = column[Option[JsObject]]("result")

      def createdOn = column[DateTime]("created_on")

      def modifiedOn = column[DateTime]("modified_on")

      def createdById = column[Option[Long]]("created_by")

      /** projection to/from the database */
      def * = (id, name, arguments, error, progress, complete, result, createdOn, modifiedOn, createdById) <> (gaoQuery.tupled, gaoQuery.unapply)

      def createdBy = foreignKey("query_created_by", createdById, users)(_.id)
    }

    val queries = TableQuery[QueryTable]

    protected val queriesAutoInc = queries returning queries.map(_.id) into {
      case (f, id) => f.copy(id = id)
    }

    override def insert(query: QueryTemplate)(implicit session: Session): Try[gaoQuery] = Try {
      // TODO: add createdBy user id
      val c = gaoQuery(0, query.name, query.arguments, None, List(), false, None, new DateTime(), new DateTime(), None)
      queriesAutoInc.insert(c)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      queries.where(_.id === id).mutate(f => f.delete())
    }

    override def update(query: gaoQuery)(implicit session: Session): Try[gaoQuery] = Try {
      val updatedQuery = query.copy(modifiedOn = new DateTime())
      val updated = queries.where(_.id === query.id).update(updatedQuery)
      updatedQuery
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[gaoQuery] = {
      queries.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[gaoQuery] = {
      queries.where(_.id === id).firstOption
    }

    override def lookupByName(name: String)(implicit session: Session): Option[gaoQuery] = {
      queries.where(_.name === name).firstOption
    }

    /**
     * update the query to complete
     * @param id query id
     * @param complete the complete flag
     * @param session session to db
     */
    override def updateComplete(id: Long, complete: Boolean)(implicit session: Session): Try[Unit] = Try {
      val completeCol = for (c <- queries if c.id === id) yield c.complete
      completeCol.update(complete)
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      queries.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      queries.ddl.drop
    }

    /**
     * update the progress for the command
     * @param id command id
     * @param progress progress for the command
     * @param session session to db
     */
    override def updateProgress(id: Long, progress: List[Float])(implicit session: Session): Try[Unit] = Try {
      val q = for { c <- queries if c.id === id } yield c.progress
      q.update(progress)
    }
  }


  /**
   * A slick implementation of the graph repository. It stores metadata for graphs.
   *
   * Currently graph metadata consists only of an (id, name) pair. We could add the schema information if people
   * think that would be helpful but beware: That sort of thing mutates as the graph evolves so keeping it current
   * will require tracking.
   */
  class SlickGraphRepository extends Repository[Session, GraphTemplate, Graph]
      with EventLogging {
    this: Repository[Session, GraphTemplate, Graph] =>

    class GraphTable(tag: Tag) extends Table[Graph](tag, "graph") {
      def id = column[Long]("graph_id", O.PrimaryKey, O.AutoInc)

      def name = column[String]("name")

      def description = column[Option[String]]("description")

      /** name used in physical data store, the HBase table name */
      def storage = column[String]("storage")

      def statusId = column[Long]("status_id", O.Default(1))

      def createdOn = column[DateTime]("created_on")

      def modifiedOn = column[DateTime]("modified_on")

      def createdByUserId = column[Option[Long]]("created_by")

      def modifiedByUserId = column[Option[Long]]("modified_by")

      /** projection to/from the database */
      override def * = (id, name, description, storage, statusId, createdOn, modifiedOn, createdByUserId, modifiedByUserId) <> (Graph.tupled, Graph.unapply)

      // foreign key relationships

      def status = foreignKey("graph_status_id", statusId, statuses)(_.id)

      def createdBy = foreignKey("graph_created_by", createdByUserId, users)(_.id)

      def modifiedBy = foreignKey("graph_modified_by", modifiedByUserId, users)(_.id)
    }

    val graphs = TableQuery[GraphTable]

    protected val graphsAutoInc = graphs returning graphs.map(_.id) into {
      case (graph, id) => graph.copy(id = id)
    }

    override def insert(graph: GraphTemplate)(implicit session: Session): Try[Graph] = Try {
      // TODO: table name
      // TODO: user name
      val g = Graph(1, graph.name, None, "", 1L, new DateTime(), new DateTime(), None, None)
      graphsAutoInc.insert(g)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      graphs.where(_.id === id).mutate(f => f.delete())
    }

    override def update(graph: Graph)(implicit session: Session): Try[Graph] = Try {
      val updatedGraph = graph.copy(modifiedOn = new DateTime)
      graphs.where(_.id === graph.id).update(updatedGraph)
      updatedGraph
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[Graph] = {
      graphs.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[Graph] = {
      graphs.where(_.id === id).firstOption
    }

    override def lookupByName(name: String)(implicit session: Session): Option[Graph] = {
      graphs.where(_.name === name).firstOption
    }
    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      graphs.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      graphs.ddl.drop
    }

  }

}
