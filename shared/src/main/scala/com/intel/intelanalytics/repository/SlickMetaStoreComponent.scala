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

package com.intel.intelanalytics.repository

import java.util.concurrent.TimeUnit

import com.github.tototoshi.slick.GenericJodaSupport
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import com.intel.intelanalytics.domain.gc.{ GarbageCollectionEntryTemplate, GarbageCollectionEntry, GarbageCollection, GarbageCollectionTemplate }
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.graph.{ GraphEntity, GraphTemplate }
import com.intel.intelanalytics.domain.model.{ ModelTemplate, ModelEntity }
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.domain.query.{ QueryTemplate, Query => QueryRecord }
import com.intel.intelanalytics.domain.schema.Schema
import com.typesafe.config.ConfigFactory
import org.joda.time.{ Duration, DateTime }
import com.intel.intelanalytics.domain.schema.{ VertexSchema, EdgeSchema, FrameSchema, Schema }
import org.joda.time.DateTime
import scala.slick.driver.{ JdbcDriver, JdbcProfile }
import org.flywaydb.core.Flyway
import spray.json._
import scala.util.Try
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.ProgressInfo
import scala.Some
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.domain.Status
import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.domain.Error
import com.intel.intelanalytics.domain.UserTemplate
import com.intel.event.{ EventContext, EventLogging }
import scala.Some
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.engine.ProgressInfo
import com.intel.intelanalytics.domain.schema.FrameSchema
import com.intel.intelanalytics.domain.query.QueryTemplate
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.domain.model.ModelEntity
import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.schema.EdgeSchema
import com.intel.intelanalytics.domain.graph.GraphEntity
import com.intel.intelanalytics.domain.graph.SchemaList
import com.intel.intelanalytics.domain.model.ModelTemplate
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.domain.Error
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.UserTemplate
import com.intel.intelanalytics.domain.schema.VertexSchema

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

  implicit val commandProgressType = MappedColumnType.base[List[ProgressInfo], String](
    { progress => progress.toJson.prettyPrint },
    { string => JsonParser(string).convertTo[List[ProgressInfo]] }
  )

  implicit val elementIDNamesColumnType = MappedColumnType.base[SchemaList, String](
    { elementIDNames => elementIDNames.toJson.prettyPrint }, // Schema to String
    { string => JsonParser(string).convertTo[SchemaList] } // String to Schema
  )

  private[repository] val database = withContext("Connecting to database") {
    info("JDBC Connection String: " + profile.connectionString)
    info("JDBC Driver: " + profile.driver)
    Database.forURL(profile.connectionString, driver = profile.driver, user = profile.username, password = profile.password)
  }(null)

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

      withSession("initializing/verifying schema") {
        implicit session =>
          if (profile.isH2) {
            info("Creating schema using H2")
            // Tables that are dependencies for other tables need to go first
            statusRepo.asInstanceOf[SlickStatusRepository].createTable
            statusRepo.asInstanceOf[SlickStatusRepository].initializeValues
            userRepo.asInstanceOf[SlickUserRepository].createTable
            modelRepo.asInstanceOf[SlickModelRepository].createTable
            commandRepo.asInstanceOf[SlickCommandRepository].createTable // depends on user
            graphRepo.asInstanceOf[SlickGraphRepository].createTable // depends on user, status
            frameRepo.asInstanceOf[SlickFrameRepository].createTable // depends on user, status
            queryRepo.asInstanceOf[SlickQueryRepository].createTable // depends on user
            gcRepo.asInstanceOf[SlickGarbageCollectionRepository].createTable
            gcEntryRepo.asInstanceOf[SlickGarbageCollectionEntryRepository].createTable //depends on gc

            info("Schema creation completed")

            //populate the database with some test users from the specified file (for testing), read from the resources folder
            val apiKey = "test_api_key_1"
            info(s"Creating test user with api key $apiKey")
            userRepo.insert(new UserTemplate(apiKey)).get
            assert(userRepo.scan().length > 0, "No user was created")
            assert(userRepo.retrieveByColumnValue("api_key", apiKey).length == 1, "User not found by api key")
          }
          else {
            info("Running migrations to create/update schema as needed, jdbcUrl: " + profile.connectionString +
              ", user: " + profile.username)
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
            modelRepo.asInstanceOf[SlickModelRepository].dropTable
            gcEntryRepo.asInstanceOf[SlickGarbageCollectionEntryRepository].dropTable //depends on gc
            gcRepo.asInstanceOf[SlickGarbageCollectionRepository].dropTable
            info("tables dropped")
          }
          else {
            throw new RuntimeException("Dropping tables is only supported for H2")
          }
      }
    }

    override lazy val statusRepo: Repository[Session, Status, Status] = new SlickStatusRepository

    override lazy val frameRepo: FrameRepository[Session] = new SlickFrameRepository

    override lazy val graphRepo: GraphRepository[Session] = new SlickGraphRepository(frameRepo)

    override lazy val modelRepo: ModelRepository[Session] = new SlickModelRepository

    override lazy val gcRepo: GarbageCollectionRepository[Session] = new SlickGarbageCollectionRepository

    override lazy val gcEntryRepo: GarbageCollectionEntryRepository[Session] = new SlickGarbageCollectionEntryRepository

    /** Repository for CRUD on 'command' table */
    override lazy val commandRepo: CommandRepository[Session] = new SlickCommandRepository

    /** Repository for CRUD on 'command' table */
    override lazy val queryRepo: QueryRepository[Session] = new SlickQueryRepository

    /** Repository for CRUD on 'user' table */
    override lazy val userRepo: Repository[Session, UserTemplate, User] with Queryable[Session, User] = new SlickUserRepository

    override def withSession[T](name: String)(f: (Session) => T)(implicit evc: EventContext = EventContext.getCurrent()): T = {
      withContext(name) {
        database.withSession(f)
      }
    }

    override def withTransaction[T](name: String)(f: (Session) => T)(implicit evc: EventContext = EventContext.getCurrent()): T = {
      withContext(name) {
        database.withTransaction(f)
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
      with NameableRepository[Session, User]
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

    override def lookupByName(name: Option[String])(implicit session: Session): Option[User] = {
      name match {
        case Some(n) => users.where(_.username === n).firstOption
        case _ => None
      }
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

    // (Status.apply _).tupled is how you do it when you have a companion object

    /** projection to/from the database */
    def * = (id, name, description, createdOn, modifiedOn) <> ((Status.apply _).tupled, Status.unapply)
  }

  val statuses = TableQuery[StatusTable]

  /**
   * A slick implementation of the status repository. It stores metadata for statuses.
   */
  class SlickStatusRepository extends Repository[Session, Status, Status] with NameableRepository[Session, Status]
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

    override def lookupByName(name: Option[String])(implicit session: Session): Option[Status] = {
      name match {
        case Some(n) => statuses.where(_.name === n).firstOption
        case _ => None
      }
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
      insert(Status(6, "LIVE", "Active and used.", new DateTime(), new DateTime()))
      insert(Status(7, "WEAKLY_LIVE", "Active but unused. The data on disk may be deleted but can be recreated", new DateTime(), new DateTime()))
      insert(Status(8, "DEAD", "INACTIVE AND UNUSED, the data on disk will be deleted but can be recreated", new DateTime(), new DateTime()))
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      statuses.ddl.drop
    }
  }

  /**
   * A slick implementation of the 'Frame' table that defines
   * the columns and conversion to/from Scala beans.
   */
  class FrameTable(tag: Tag) extends Table[FrameEntity](tag, "frame") {
    def id = column[Long]("frame_id", O.PrimaryKey, O.AutoInc)

    def name = column[Option[String]]("name")

    def description = column[Option[String]]("description")

    def schema = column[Schema]("schema")

    def rowCount = column[Option[Long]]("row_count")
    def statusId = column[Long]("status_id", O.Default(1))

    def createdOn = column[DateTime]("created_on")

    def createdById = column[Option[Long]]("created_by")
    def modifiedOn = column[DateTime]("modified_on")

    def modifiedById = column[Option[Long]]("modified_by")

    def errorFrameId = column[Option[Long]]("error_frame_id")

    def revision = column[Int]("revision")

    def commandId = column[Option[Long]]("command_id")
    def graphId = column[Option[Long]]("graph_id")

    def materializedOn = column[Option[DateTime]]("materialized_start")

    def materializationComplete = column[Option[DateTime]]("materialized_end")

    def storageFormat = column[Option[String]]("storage_format")

    def storageLocation = column[Option[String]]("storage_uri")

    def parentId = column[Option[Long]]("parent_frame_id")

    def lastReadDate = column[DateTime]("last_read_date")

    /** projection to/from the database */
    override def * = (id, name, schema, statusId, createdOn, modifiedOn,
      storageFormat, storageLocation, description, rowCount, commandId, createdById, modifiedById,
      materializedOn, materializationComplete,
      errorFrameId, parentId, graphId, lastReadDate) <> (FrameEntity.tupled, FrameEntity.unapply)

    // foreign key relationships

    def status = foreignKey("frame_status_id", statusId, statuses)(_.id)

    def createdBy = foreignKey("frame_created_by", createdById, users)(_.id)

    def modifiedBy = foreignKey("frame_modified_by", modifiedById, users)(_.id)

    def errorFrame = foreignKey("frame_error_frame_id", errorFrameId, frames)(_.id)

    def command = foreignKey("frame_command_id", commandId, commandTable)(_.id)

    def parent = foreignKey("frame_parent_id", parentId, frames)(_.id)

    def graph = foreignKey("graph_id", graphId, graphs)(_.id)

  }

  val frames = TableQuery[FrameTable]

  /**
   * A slick implementation of a Frame Repository.
   *
   * Provides methods for modifying and querying the frame table.
   */
  class SlickFrameRepository extends FrameRepository[Session]
      with EventLogging {
    this: Repository[Session, DataFrameTemplate, FrameEntity] =>
    type Session = msc.Session

    protected val framesAutoInc = frames returning frames.map(_.id) into {
      case (f, id) => f.copy(id = id)
    }

    def _insertFrame(frame: DataFrameTemplate)(implicit session: Session) = {
      val now: DateTime = new DateTime()
      val f = FrameEntity(id = 0, name = frame.name, description = frame.description,
        schema = FrameSchema(), status = 1L, createdOn = now, modifiedOn = now, rowCount = Some(0))
      framesAutoInc.insert(f)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      // if you are deleting an error frame, you need to make sure no other frames reference it first
      val errorFrameIdColumn = for (f <- frames if f.errorFrameId === id) yield f.errorFrameId
      errorFrameIdColumn.update(None)

      // if you are deleting any frame, you need to make sure no other frames reference it as a parent first
      val parentIdColumn = for (f <- frames if f.parentId === id) yield f.parentId
      parentIdColumn.update(None)

      // perform the actual delete
      frames.where(_.id === id).mutate(f => f.delete())
    }

    override def update(frame: FrameEntity)(implicit session: Session): Try[FrameEntity] = Try {
      val updatedFrame = frame.copy(modifiedOn = new DateTime)
      frames.where(_.id === frame.id).update(updatedFrame)
      updatedFrame
    }

    //TODO: All these updates should update the modifiedOn and modifiedBy fields
    override def updateSchema(frame: FrameEntity, schema: Schema)(implicit session: Session): FrameEntity = {
      if (frame.isVertexFrame) {
        require(schema.isInstanceOf[VertexSchema], s"vertex frame requires schema to be of type vertex schema but found ${schema.getClass.getName}")
      }
      else if (frame.isEdgeFrame) {
        require(schema.isInstanceOf[EdgeSchema], s"edge frame requires schema to be of type edge schema but found ${schema.getClass.getName}")
      }
      else {
        require(schema.isInstanceOf[FrameSchema], s"frame requires schema to be of type frame schema but found ${schema.getClass.getName}")
      }

      // this looks crazy but it is how you update only one column
      val schemaColumn = for (f <- frames if f.id === frame.id) yield f.schema
      schemaColumn.update(schema)
      frames.where(_.id === frame.id).firstOption.get
    }

    override def updateRowCount(frame: FrameEntity, rowCount: Option[Long])(implicit session: Session): FrameEntity = {
      // this looks crazy but it is how you update only one column
      val rowCountColumn = for (f <- frames if f.id === frame.id) yield f.rowCount
      rowCountColumn.update(rowCount)
      frames.where(_.id === frame.id).firstOption.get
    }

    /** Update the errorFrameId column */
    override def updateErrorFrameId(frame: FrameEntity, errorFrameId: Option[Long])(implicit session: Session): FrameEntity = {
      // this looks crazy but it is how you update only one column
      val errorFrameIdColumn = for (f <- frames if f.id === frame.id) yield f.errorFrameId
      errorFrameIdColumn.update(errorFrameId)
      frames.where(_.id === frame.id).firstOption.get
    }

    //    override def updateRevision(frame: DataFrame, revision: Int)(implicit session: Session): DataFrame = {
    //      // this looks crazy but it is how you update only one column
    //      val column = for (f <- frames if f.id === frame.id) yield f.revision
    //      column.update(revision)
    //      frames.where(_.id === frame.id).firstOption.get
    //    }

    override def insert(frame: DataFrameTemplate)(implicit session: Session): Try[FrameEntity] = Try {
      _insertFrame(frame)(session)
    }

    override def insert(frame: FrameEntity)(implicit session: Session): FrameEntity = {
      framesAutoInc.insert(frame)
    }

    override def scanAll()(implicit session: Session): Seq[FrameEntity] = {
      frames.list
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[FrameEntity] = {
      frames.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[FrameEntity] = {
      frames.where(_.id === id).firstOption
    }

    override def lookupByName(name: Option[String])(implicit session: Session): Option[FrameEntity] = {
      name match {
        case Some(n) => frames.where(_.name === n).firstOption
        case _ => None
      }
    }

    override def lookupByGraphId(graphId: Long)(implicit session: Session): Seq[FrameEntity] = {
      frames.where(_.graphId === graphId).list
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      frames.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      frames.ddl.drop
    }

    /**
     * update and mark an entity as having it's data deleted
     * @param frame entity to be deleted
     * @param session the user session
     * @return the entity
     */
    override def updateDataDeleted(frame: FrameEntity)(implicit session: Session): Try[FrameEntity] = Try {
      val cols = frames.filter(_.id === frame.id).map(f => (f.statusId, f.materializedOn, f.materializationComplete, f.modifiedOn))
      cols.update((Status.Dead, None, None, new DateTime))
      frames.where(_.id === frame.id).firstOption.get
    }

    /**
     * Return a list of entities ready to delete
     * @param age the length of time in milliseconds for the newest possible record to be deleted
     * @param session current session
     */
    override def listReadyForDeletion(age: Long)(implicit session: Session): Seq[FrameEntity] = {
      val oldestDate = DateTime.now.minus(age)
      val list = (for (
        f <- frames; if f.name.isNull &&
          f.statusId =!= Status.Dead &&
          f.statusId =!= Status.Deleted &&
          f.lastReadDate < oldestDate
      ) yield f).list
      list.filter(f => !inLiveGraph(f))
    }

    /**
     * Return true if the supplied frame is part of a live graph
     * @param frame frame in question
     * @param session the user session
     */
    def inLiveGraph(frame: FrameEntity)(implicit session: Session): Boolean = {
      frame.graphId match {
        case None => false
        case Some(g) => {
          val graph = metaStore.graphRepo.lookup(g)(session.asInstanceOf[metaStore.Session])
          metaStore.graphRepo.isLive(graph.get)(session.asInstanceOf[metaStore.Session])
        }
      }
    }

    /**
     * determines if a frame is live from its metadat
     * @param frame frame in question
     */
    override def isLive(frame: FrameEntity): Boolean = {
      frame.name.isDefined && frame.status != Status.Dead && frame.status != Status.Deleted
    }

    /**
     * Return a list of entities ready to delete metadata
     * @param age the length of time in milliseconds for the newest possible record to be deleted
     * @param session current session
     */
    override def listReadyForMetaDataDeletion(age: Long)(implicit session: Session): Seq[FrameEntity] = {
      val oldestDate = DateTime.now.minus(age)
      val list = (for (f <- frames; if f.name.isNull && f.statusId === Status.Dead && f.lastReadDate < oldestDate) yield f).list
      list.filter(f => !inLiveGraph(f) && !hasLiveChildren(f.id))
    }

    /**
     * return true if the id supplied belongs to an entity that has live children
     * @param id
     * @param session
     */
    def hasLiveChildren(id: Long)(implicit session: Session): Boolean = {
      val list = frames.where(f => f.parentId === id &&
        f.statusId =!= Status.Dead &&
        f.statusId =!= Status.Deleted).list
      list.filter(f => !hasLiveChildren(f.id)).length > 0
    }

    /**
     * update and mark an entity as having it's metadata deleted
     * @param frame entity to be deleted
     * @param session the user session
     * @return the entity marked as deleted
     */
    override def updateMetaDataDeleted(frame: FrameEntity)(implicit session: Session): Try[FrameEntity] = Try {
      val cols = frames.filter(_.id === frame.id).map(f => (f.statusId, f.materializedOn, f.materializationComplete, f.modifiedOn))
      cols.update((Status.Deleted, None, None, new DateTime))
      frames.where(_.id === frame.id).firstOption.get
    }

    /**
     * return true if the id supplied belongs to an entity that has children
     * @param id
     * @param session
     */
    def hasChildren(id: Long)(implicit session: Session): Boolean = {
      frames.where(_.parentId === id).list.length > 0
    }

    /**
     * update the last read data of an entity if it has been marked as deleted change it's status
     * @param entity entity to be updated
     * @param session the user session
     */
    def updateLastReadDate(entity: FrameEntity)(implicit session: Session): Try[FrameEntity] = Try {
      val columns = frames.filter(_.id === entity.id).map(f => (f.lastReadDate, f.statusId, f.modifiedOn))
      columns.update((new DateTime, Status.getNewStatusForRead(entity.status), new DateTime))
      frames.where(_.id === entity.id).firstOption.get
    }
  }

  /**
   * A slick implementation of the 'Command' table that defines
   * the columns and conversion to/from Scala beans.
   */
  class CommandTable(tag: Tag) extends Table[Command](tag, "command") {
    def id = column[Long]("command_id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def arguments = column[Option[JsObject]]("arguments")

    def correlationId = column[String]("correlation")

    def error = column[Option[Error]]("error")

    def progress = column[List[ProgressInfo]]("progress")

    def complete = column[Boolean]("complete", O.Default(false))

    def result = column[Option[JsObject]]("result")

    def createdOn = column[DateTime]("created_on")

    def modifiedOn = column[DateTime]("modified_on")

    def createdById = column[Option[Long]]("created_by")

    /** projection to/from the database */
    def * = (id, name, arguments, correlationId, error, progress, complete, result, createdOn, modifiedOn, createdById) <>
      (Command.tupled, Command.unapply)

    def createdBy = foreignKey("command_created_by", createdById, users)(_.id)
  }

  val commandTable = TableQuery[CommandTable]

  /**
   * A slick implementation of a Command Repository.
   *
   * Provides methods for modifying and querying the command table.
   */
  class SlickCommandRepository extends CommandRepository[Session] with NameableRepository[Session, Command]
      with EventLogging {
    this: Repository[Session, CommandTemplate, Command] =>

    protected val commandsAutoInc = commandTable returning commandTable.map(_.id) into {
      case (f, id) => f.copy(id = id)
    }

    override def insert(command: CommandTemplate)(implicit session: Session): Try[Command] = Try {
      // TODO: add createdBy user id
      val c = Command(0, command.name, command.arguments, "", None, List(), complete = false, None, new DateTime(), new DateTime(), command.createdBy)
      commandsAutoInc.insert(c)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      commandTable.where(_.id === id).mutate(f => f.delete())
    }

    override def update(command: Command)(implicit session: Session): Try[Command] = Try {
      val updatedCommand = command.copy(modifiedOn = new DateTime())
      val updated = commandTable.where(_.id === command.id).update(updatedCommand)
      updatedCommand
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[Command] = {
      //Since sortBy.drop.take seems to be producing results in random order, try this...
      commandTable.filter(_.id >= offset.toLong)
        .filter(_.id < (offset + count).toLong)
        .sortBy(_.id).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[Command] = {
      commandTable.where(_.id === id).firstOption
    }

    override def lookupByName(name: Option[String])(implicit session: Session): Option[Command] = {
      name match {
        case Some(n) => commandTable.where(_.name === n).firstOption
        case _ => None
      }
    }

    /**
     * update the command to complete
     * @param id command id
     * @param complete the complete flag
     * @param session session to db
     */
    override def updateComplete(id: Long, complete: Boolean)(implicit session: Session): Try[Unit] = Try {
      val completeCol = for (c <- commandTable if c.id === id) yield c.complete
      completeCol.update(complete)
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      commandTable.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      commandTable.ddl.drop
    }

    /**
     * update the progress for the command
     * @param id command id
     * @param progress progress for the command
     * @param session session to db
     */
    override def updateProgress(id: Long, progress: List[ProgressInfo])(implicit session: Session): Try[Unit] = Try {
      val q = for { c <- commandTable if c.id === id && c.complete === false } yield c.progress
      q.update(progress)
    }
  }

  /**
   * A slick implementation of a Query Repository.
   *
   * Provides methods for modifying and querying the query table.
   */
  class SlickQueryRepository extends QueryRepository[Session] with NameableRepository[Session, QueryRecord]
      with EventLogging {
    this: Repository[Session, QueryTemplate, QueryRecord] =>

    /**
     * A slick implementation of the 'Query' table that defines
     * the columns and conversion to/from Scala beans.
     */
    class QueryTable(tag: Tag) extends Table[QueryRecord](tag, "query") {
      def id = column[Long]("query_id", O.PrimaryKey, O.AutoInc)

      def name = column[String]("name")

      def arguments = column[Option[JsObject]]("arguments")

      def error = column[Option[Error]]("error")

      def complete = column[Boolean]("complete", O.Default(false))

      def totalPages = column[Option[Long]]("total_pages")

      def pageSize = column[Option[Long]]("page_size")

      def createdOn = column[DateTime]("created_on")

      def modifiedOn = column[DateTime]("modified_on")

      def createdById = column[Option[Long]]("created_by")

      /** projection to/from the database */
      def * = (id, name, arguments, error, complete, totalPages, pageSize, createdOn, modifiedOn, createdById) <> (QueryRecord.tupled, QueryRecord.unapply)

      def createdBy = foreignKey("query_created_by", createdById, users)(_.id)
    }

    val queries = TableQuery[QueryTable]

    protected val queriesAutoInc = queries returning queries.map(_.id) into {
      case (f, id) => f.copy(id = id)
    }

    override def insert(query: QueryTemplate)(implicit session: Session): Try[QueryRecord] = Try {
      // TODO: add createdBy user id
      val c = QueryRecord(0, query.name, query.arguments, None, complete = false, None, None, new DateTime(), new DateTime(), None)
      queriesAutoInc.insert(c)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      queries.where(_.id === id).mutate(f => f.delete())
    }

    override def update(query: QueryRecord)(implicit session: Session): Try[QueryRecord] = Try {
      val updatedQuery = query.copy(modifiedOn = new DateTime())
      val updated = queries.where(_.id === query.id).update(updatedQuery)
      updatedQuery
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[QueryRecord] = {
      queries.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[QueryRecord] = {
      queries.where(_.id === id).firstOption
    }

    override def lookupByName(name: Option[String])(implicit session: Session): Option[QueryRecord] = {
      name match {
        case Some(n) => queries.where(_.name === n).firstOption
        case _ => None
      }
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
  }

  class GraphTable(tag: Tag) extends Table[GraphEntity](tag, "graph") {
    def id = column[Long]("graph_id", O.PrimaryKey, O.AutoInc)

    def name = column[Option[String]]("name")

    def description = column[Option[String]]("description")

    /** name used in physical data store, the HBase table name */
    def storage = column[String]("storage")

    def statusId = column[Long]("status_id", O.Default(1))

    def storageFormat = column[String]("storage_format")

    def createdOn = column[DateTime]("created_on")

    def modifiedOn = column[DateTime]("modified_on")

    def createdByUserId = column[Option[Long]]("created_by")

    def modifiedByUserId = column[Option[Long]]("modified_by")

    def idCounter = column[Option[Long]]("id_counter")

    def lastReadDate = column[DateTime]("last_read_date")

    /** projection to/from the database */
    override def * = (id, name, description, storage, statusId, storageFormat, createdOn, modifiedOn, createdByUserId, modifiedByUserId, idCounter, lastReadDate) <> (GraphEntity.tupled, GraphEntity.unapply)

    // foreign key relationships

    def status = foreignKey("graph_status_id", statusId, statuses)(_.id)

    def createdBy = foreignKey("graph_created_by", createdByUserId, users)(_.id)

    def modifiedBy = foreignKey("graph_modified_by", modifiedByUserId, users)(_.id)
  }

  val graphs = TableQuery[GraphTable]

  /**
   * A slick implementation of the graph repository. It stores metadata for graphs.
   *
   * Currently graph metadata consists only of an (id, name) pair. We could add the schema information if people
   * think that would be helpful but beware: That sort of thing mutates as the graph evolves so keeping it current
   * will require tracking.
   */
  class SlickGraphRepository(frameRepo: FrameRepository[Session]) extends GraphRepository[Session]
      with EventLogging {
    this: Repository[Session, GraphTemplate, GraphEntity] =>

    protected val graphsAutoInc = graphs returning graphs.map(_.id) into {
      case (graph, id) => graph.copy(id = id)
    }

    override def insert(graph: GraphTemplate)(implicit session: Session): Try[GraphEntity] = Try {
      // TODO: table name
      // TODO: user name
      val g = GraphEntity(1, graph.name, None, "", 1L, graph.storageFormat, new DateTime(), new DateTime(), None, None)
      graphsAutoInc.insert(g)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      // for a seamless graph you also need to delete all of the frames
      frames.where(_.graphId === id).foreach(frame => frameRepo.delete(frame.id))

      graphs.where(_.id === id).mutate(f => f.delete())
    }

    override def update(graph: GraphEntity)(implicit session: Session): Try[GraphEntity] = Try {
      val updatedGraph = graph.copy(modifiedOn = new DateTime)
      graphs.where(_.id === graph.id).update(updatedGraph)
      updatedGraph
    }

    override def updateIdCounter(id: Long, idCounter: Long)(implicit session: Session): Unit = {
      val idCounterCol = for (g <- graphs if g.id === id) yield g.idCounter
      idCounterCol.update(Some(idCounter))
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[GraphEntity] = {
      graphs.drop(offset).take(count).list
    }

    override def scanAll()(implicit session: Session): Seq[GraphEntity] = {
      graphs.list
    }

    override def lookup(id: Long)(implicit session: Session): Option[GraphEntity] = {
      graphs.where(_.id === id).firstOption
    }

    override def lookupByName(name: Option[String])(implicit session: Session): Option[GraphEntity] = {
      name match {
        case Some(n) => graphs.where(_.name === n).firstOption
        case _ => None
      }
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      graphs.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      graphs.ddl.drop
    }

    /**
     * return true if the supplied graph is live.
     * @param g the graph in question
     */
    override def isLive(g: GraphEntity)(implicit session: Session): Boolean = {
      val live = g.name.isDefined && g.statusId != Status.Dead && g.statusId != Status.Deleted
      if (live) {
        true
      }
      else {
        val frames = metaStore.frameRepo.lookupByGraphId(g.id)(session.asInstanceOf[metaStore.Session])
        frames.filter(f => metaStore.frameRepo.isLive(f)).length > 0
      }
    }

    /**
     * Return a list of entities ready to delete
     * @param age the length of time in milliseconds for the newest possible record to be deleted
     * @param session current session
     */
    override def listReadyForDeletion(age: Long)(implicit session: Session): Seq[GraphEntity] = {
      val oldestDate = DateTime.now.minus(age)
      val list = (for (
        g <- graphs; if g.name.isNull &&
          g.statusId =!= Status.Dead &&
          g.statusId =!= Status.Deleted &&
          g.lastReadDate < oldestDate
      ) yield g).list
      list.filter(g => !isLive(g))
    }

    /**
     * update and mark an entity as having it's metadata deleted
     * @param entity entity to be deleted
     * @param session the user session
     * @return the entity marked as deleted
     */
    override def updateMetaDataDeleted(entity: GraphEntity)(implicit session: Session): Try[GraphEntity] = Try {
      graphs.filter(_.id === entity.id)
        .map(g => (g.statusId, g.modifiedOn))
        .update((Status.Deleted, new DateTime))
      graphs.where(_.id === entity.id).firstOption.get
    }

    /**
     * Return a list of entities ready to delete metadata
     * @param age the length of time in milliseconds for the newest possible record to be deleted
     * @param session current session
     */
    override def listReadyForMetaDataDeletion(age: Long)(implicit session: Session): Seq[GraphEntity] = {
      val oldestDate = DateTime.now.minus(age)
      (for (g <- graphs; if g.name.isNull && g.statusId === Status.Dead && g.lastReadDate < oldestDate) yield g).list
    }

    /**
     * update and mark an entity as having it's data deleted
     * @param entity entity to be deleted
     * @param session the user session
     * @return the entity
     */
    override def updateDataDeleted(entity: GraphEntity)(implicit session: Session): Try[GraphEntity] = Try {
      graphs.filter(_.id === entity.id)
        .map(g => (g.statusId, g.modifiedOn))
        .update((Status.Dead, new DateTime))
      graphs.where(_.id === entity.id).firstOption.get
    }

    /**
     * update the last read data of an entity if it has been marked as deleted change it's status
     * @param entity entity to be updated
     * @param session the user session
     */
    def updateLastReadDate(entity: GraphEntity)(implicit session: Session): Try[GraphEntity] = Try {
      graphs.filter(_.id === entity.id)
        .map(g => (g.lastReadDate, g.statusId, g.modifiedOn))
        .update((new DateTime, Status.getNewStatusForRead(entity.statusId), new DateTime))
      graphs.where(_.id === entity.id).firstOption.get
    }
  }

  class SlickModelRepository extends ModelRepository[Session]
      with EventLogging {
    this: Repository[Session, ModelTemplate, ModelEntity] =>

    class ModelTable(tag: Tag) extends Table[ModelEntity](tag, "model") {
      def id = column[Long]("model_id", O.PrimaryKey, O.AutoInc)

      def name = column[Option[String]]("name")

      def modelType = column[String]("model_type")

      def description = column[Option[String]]("description")

      def statusId = column[Long]("status_id", O.Default(Status.Active))

      def data = column[Option[JsObject]]("data")

      def createdOn = column[DateTime]("created_on")

      def modifiedOn = column[DateTime]("modified_on")

      def createdByUserId = column[Option[Long]]("created_by")

      def modifiedByUserId = column[Option[Long]]("modified_by")

      def lastReadDate = column[DateTime]("last_read_date")

      /** projection to/from the database */
      override def * = (id, name, modelType, description, statusId, data, createdOn, modifiedOn, createdByUserId, modifiedByUserId, lastReadDate) <> (ModelEntity.tupled, ModelEntity.unapply)

    }

    val models = TableQuery[ModelTable]

    protected val modelsAutoInc = models returning models.map(_.id) into {
      case (model, id) => model.copy(id = id)
    }

    override def insert(model: ModelTemplate)(implicit session: Session): Try[ModelEntity] = Try {
      // TODO: table name
      // TODO: user name
      val m = ModelEntity(1, model.name, model.modelType, None, Status.Active, None, new DateTime(), new DateTime(), None, None)
      modelsAutoInc.insert(m)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      models.where(_.id === id).mutate(f => f.delete())
    }

    override def update(model: ModelEntity)(implicit session: Session): Try[ModelEntity] = Try {
      val updatedModel = model.copy(modifiedOn = new DateTime)
      models.where(_.id === model.id).update(updatedModel)
      updatedModel
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[ModelEntity] = {
      models.drop(offset).take(count).list
    }

    override def scanAll()(implicit session: Session): Seq[ModelEntity] = {
      models.list
    }

    override def lookup(id: Long)(implicit session: Session): Option[ModelEntity] = {
      models.where(_.id === id).firstOption
    }

    override def lookupByName(name: Option[String])(implicit session: Session): Option[ModelEntity] = {
      name match {
        case Some(n) => models.where(_.name === n).firstOption
        case _ => None
      }
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      models.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      models.ddl.drop
    }

    /**
     * Return a list of entities ready to delete
     * @param age the length of time in milliseconds for the newest possible record to be deleted
     * @param session current session
     */
    override def listReadyForDeletion(age: Long)(implicit session: Session): Seq[ModelEntity] = {
      val oldestDate = DateTime.now.minus(age)
      (for (
        m <- models; if m.name.isNull &&
          m.statusId =!= Status.Dead &&
          m.statusId =!= Status.Deleted &&
          m.lastReadDate < oldestDate
      ) yield m).list
    }

    /**
     * update and mark an entity as having it's metadata deleted
     * @param entity entity to be deleted
     * @param session the user session
     * @return the entity marked as deleted
     */
    override def updateMetaDataDeleted(entity: ModelEntity)(implicit session: Session): Try[ModelEntity] = Try {
      models.filter(_.id === entity.id)
        .map(m => (m.statusId, m.modifiedOn))
        .update((Status.Deleted, new DateTime))
      models.where(_.id === entity.id).firstOption.get
    }

    /**
     * Return a list of entities ready to delete metadata
     * @param age the length of time in milliseconds for the newest possible record to be deleted
     * @param session current session
     */
    override def listReadyForMetaDataDeletion(age: Long)(implicit session: Session): Seq[ModelEntity] = {
      val oldestDate = DateTime.now.minus(age)
      (for (m <- models; if m.name.isNull && m.statusId === Status.Dead && m.lastReadDate < oldestDate) yield m).list
    }

    /**
     * update and mark an entity as having it's data deleted
     * @param entity entity to be deleted
     * @param session the user session
     * @return the entity
     */
    override def updateDataDeleted(entity: ModelEntity)(implicit session: Session): Try[ModelEntity] = Try {
      models.filter(_.id === entity.id)
        .map(m => (m.statusId, m.modifiedOn))
        .update((Status.Dead, new DateTime))
      models.where(_.id === entity.id).firstOption.get
    }

    /**
     * update the last read data of an entity if it has been marked as deleted change it's status
     * @param entity entity to be updated
     * @param session the user session
     */
    def updateLastReadDate(entity: ModelEntity)(implicit session: Session): Try[ModelEntity] = Try {
      models.filter(_.id === entity.id)
        .map(m => (m.lastReadDate, m.statusId, m.modifiedOn))
        .update((new DateTime, Status.getNewStatusForRead(entity.statusId), new DateTime))
      models.where(_.id === entity.id).firstOption.get
    }
  }

  /**
   * Repository for GarbageCollections
   */
  class SlickGarbageCollectionRepository extends GarbageCollectionRepository[Session]
      with EventLogging {
    this: Repository[Session, GarbageCollectionTemplate, GarbageCollection] =>

    class GarbageCollectionTable(tag: Tag) extends Table[GarbageCollection](tag, "garbage_collection") {

      def id = column[Long]("garbage_collection_id", O.PrimaryKey, O.AutoInc)

      def hostname = column[String]("hostname")

      def processId = column[Long]("process_id")

      def startTime = column[DateTime]("start_time")

      def endTime = column[Option[DateTime]]("end_time")

      def createdOn = column[DateTime]("created_on")

      def modifiedOn = column[DateTime]("modified_on")

      /** projection to/from the database */
      override def * = (id, hostname, processId, startTime, endTime, createdOn, modifiedOn) <> (GarbageCollection.tupled, GarbageCollection.unapply)

    }

    val garbageCollections = TableQuery[GarbageCollectionTable]

    protected val gcAutoInc = garbageCollections returning garbageCollections.map(_.id) into {
      case (gc, id) => gc.copy(id = id)
    }

    override def insert(gc: GarbageCollectionTemplate)(implicit session: Session): Try[GarbageCollection] = Try {
      // TODO: table name
      // TODO: user name
      val newRecord = GarbageCollection(1, gc.hostname, gc.processId, gc.startTime, None, new DateTime, new DateTime)
      gcAutoInc.insert(newRecord)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      garbageCollections.where(_.id === id).mutate(f => f.delete())
    }

    override def update(gc: GarbageCollection)(implicit session: Session): Try[GarbageCollection] = Try {
      val updatedGC = gc.copy(modifiedOn = new DateTime)
      garbageCollections.where(_.id === gc.id).update(updatedGC)
      updatedGC
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[GarbageCollection] = {
      garbageCollections.drop(offset).take(count).list
    }

    override def scanAll()(implicit session: Session): Seq[GarbageCollection] = {
      garbageCollections.list
    }

    override def lookup(id: Long)(implicit session: Session): Option[GarbageCollection] = {
      garbageCollections.where(_.id === id).firstOption
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      garbageCollections.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      garbageCollections.ddl.drop
    }

    def getCurrentExecutions()(implicit session: Session): Seq[GarbageCollection] = {
      garbageCollections.filter(_.endTime.isNull).list
    }

    override def updateEndTime(entity: GarbageCollection)(implicit session: Session): Try[GarbageCollection] = Try {
      garbageCollections.filter(_.id === entity.id)
        .map(g => (g.endTime, g.modifiedOn))
        .update((Some(new DateTime), new DateTime))
      garbageCollections.where(_.id === entity.id).firstOption.get
    }

  }

  /**
   * Repository for Garbage Collection Entries
   */
  class SlickGarbageCollectionEntryRepository extends GarbageCollectionEntryRepository[Session]
      with EventLogging {
    this: Repository[Session, GarbageCollectionEntryTemplate, GarbageCollectionEntry] =>

    class GarbageCollectionEntryTable(tag: Tag) extends Table[GarbageCollectionEntry](tag, "garbage_collection_entry") {
      def id = column[Long]("garbage_collection_id", O.PrimaryKey, O.AutoInc)

      def garbageCollectionId = column[Long]("garbage_collection_id")

      def description = column[String]("description")

      def startTime = column[DateTime]("start_time")

      def endTime = column[Option[DateTime]]("end_time")

      def createdOn = column[DateTime]("created_on")

      def modifiedOn = column[DateTime]("modified_on")

      /** projection to/from the database */
      override def * = (id, garbageCollectionId, description, startTime, endTime, createdOn, modifiedOn) <> (GarbageCollectionEntry.tupled, GarbageCollectionEntry.unapply)

    }

    val garbageCollectionEntries = TableQuery[GarbageCollectionEntryTable]

    protected val gcEntriesAutoInc = garbageCollectionEntries returning garbageCollectionEntries.map(_.id) into {
      case (gcEntry, id) => gcEntry.copy(id = id)
    }

    override def insert(entry: GarbageCollectionEntryTemplate)(implicit session: Session): Try[GarbageCollectionEntry] = Try {
      // TODO: table name
      // TODO: user name
      val newRecord = GarbageCollectionEntry(1, entry.garbageCollectionId, entry.description, entry.startTime, None, new DateTime, new DateTime)
      gcEntriesAutoInc.insert(newRecord)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      garbageCollectionEntries.where(_.id === id).mutate(f => f.delete())
    }

    override def update(gc: GarbageCollectionEntry)(implicit session: Session): Try[GarbageCollectionEntry] = Try {
      val updatedGCEntry = gc.copy(modifiedOn = new DateTime)
      garbageCollectionEntries.where(_.id === gc.id).update(updatedGCEntry)
      updatedGCEntry
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[GarbageCollectionEntry] = {
      garbageCollectionEntries.drop(offset).take(count).list
    }

    override def scanAll()(implicit session: Session): Seq[GarbageCollectionEntry] = {
      garbageCollectionEntries.list
    }

    override def lookup(id: Long)(implicit session: Session): Option[GarbageCollectionEntry] = {
      garbageCollectionEntries.where(_.id === id).firstOption
    }

    /** execute DDL to create the underlying table */
    def createTable(implicit session: Session) = {
      garbageCollectionEntries.ddl.create
    }

    /** execute DDL to drop the underlying table - for unit testing */
    def dropTable()(implicit session: Session) = {
      garbageCollectionEntries.ddl.drop
    }

    override def updateEndTime(entity: GarbageCollectionEntry)(implicit session: Session): Try[GarbageCollectionEntry] = Try {
      garbageCollectionEntries.filter(_.id === entity.id)
        .map(g => (g.endTime, g.modifiedOn))
        .update((Some(new DateTime), new DateTime))
      garbageCollectionEntries.where(_.id === entity.id).firstOption.get
    }
  }

}
