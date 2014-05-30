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

import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.domain._
import scala.util.Try
import spray.json._
import scala.slick.driver.JdbcProfile
import com.intel.intelanalytics.domain.CommandTemplate
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.domain.Schema
import com.intel.intelanalytics.domain.Command

trait DbProfileComponent {

  val profile: Profile

  /**
   * Profiles is how we abstract various back-ends like H2 vs. PostgreSQL
   */
  case class Profile(profile: JdbcProfile, connectionString: String, driver: String)

}

trait SlickMetaStoreComponent extends MetaStoreComponent with EventLogging {
  msc: MetaStoreComponent with DbProfileComponent =>

  import profile.profile.simple._
  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  protected val database = withContext("Connecting to database") {
    Database.forURL(profile.connectionString, driver = profile.driver)
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
     * Create the underlying tables
     */
    override def create(): Unit = {

      /** Repository for CRUD on 'frame' table */

      withSession("Creating tables") { implicit session =>
        info("creating")
        frameRepo.asInstanceOf[SlickFrameRepository].create
        commandRepo.asInstanceOf[SlickCommandRepository].create
        userRepo.asInstanceOf[SlickUserRepository].create
        graphRepo.asInstanceOf[SlickGraphRepository].create
        info("tables created")
      }
    }

    override lazy val graphRepo: Repository[Session, GraphTemplate, Graph] = new SlickGraphRepository

    override lazy val frameRepo: Repository[Session, DataFrameTemplate, DataFrame] = new SlickFrameRepository

    /** Repository for CRUD on 'command' table */
    override lazy val commandRepo: Repository[Session, CommandTemplate, Command] = new SlickCommandRepository

    /** Repository for CRUD on 'user' table */
    override lazy val userRepo: Repository[Session, UserTemplate, User] with Queryable[Session, User] = new SlickUserRepository

    override def withSession[T](name: String)(f: (Session) => T): T = {
      withContext(name) {
        database.withSession(f)
      }
    }
  }

  /**
   * A slick implementation of a User Repository.
   *
   * Provides methods for modifying and querying the user table.
   */
  class SlickUserRepository extends Repository[Session, UserTemplate, User]
      with Queryable[Session, User]
      with EventLogging {
    this: Repository[Session, UserTemplate, User] with Queryable[Session, User] =>

    /**
     * A slick implementation of the 'User' table that defines
     * the columns and conversion to/from Scala beans.
     */
    class UserEntity(tag: Tag) extends Table[User](tag, "users") {
      def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

      def api_key = column[String]("api_key")

      def * = (id, api_key) <>
        ((t: (Long, String)) => t match {
          case (i: Long, n: String) => User.tupled((i, n))
        },
          (f: User) => User.unapply(f) map {
            case (i, n) => (i, n)
          })
    }

    val users = TableQuery[UserEntity]

    protected val usersAutoInc = users returning users.map(_.id) into {
      case (c, id) => c.copy(id = id)
    }

    /** execute DDL to create the underlying table */
    def create()(implicit session: Session) = {
      users.ddl.create
    }

    override def insert(newUser: UserTemplate)(implicit session: Session): Try[User] = Try {
      usersAutoInc.insert(User(0, newUser.api_key))(session)
    }

    override def lookup(id: Long)(implicit session: Session): Option[User] = {
      users.where(_.id === id).firstOption
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      users.where(_.id === id).mutate(c => c.delete())
    }

    override def update(c: User)(implicit session: Session): Try[User] = Try {
      users.where(_.id === c.id).update(c)
      c
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[User] = {
      users.drop(offset).take(count).list
    }

    override def retrieveByColumnValue(colName: String, value: String)(implicit session: Session): List[User] = {
      users.filter(_.column[String](colName) === value).list
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
    class Frame(tag: Tag) extends Table[DataFrame](tag, "frame") {
      def id = column[Long]("frame_id", O.PrimaryKey, O.AutoInc)

      def name = column[String]("name")

      def schema = column[String]("schema")

      def * = (id, name, schema) <>
        ((t: (Long, String, String)) => t match {
          case (i: Long, n: String, s: String) => DataFrame.tupled((i, n, JsonParser(s).convertTo[Schema]))
        },
          (f: DataFrame) => DataFrame.unapply(f) map {
            case (i, n, s) => (i, n, s.toJson.prettyPrint)
          })
    }

    val frames = TableQuery[Frame]

    protected val framesAutoInc = frames returning frames.map(_.id) into {
      case (f, id) => f.copy(id = id)
    }

    def _insertFrame(frame: DataFrameTemplate)(implicit session: Session) = {
      val f = DataFrame(0, frame.name, frame.schema)
      framesAutoInc.insert(f)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      frames.where(_.id === id).mutate(f => f.delete())
    }

    override def update(frame: DataFrame)(implicit session: Session): Try[DataFrame] = Try {
      frames.where(_.id === frame.id).update(frame)
      frame
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

    /** execute DDL to create the underlying table */
    def create(implicit session: Session) = {
      frames.ddl.create
    }

  }

  /**
   * A slick implementation of a Command Repository.
   *
   * Provides methods for modifying and querying the command table.
   */
  class SlickCommandRepository extends Repository[Session, CommandTemplate, Command]
      with EventLogging {
    this: Repository[Session, CommandTemplate, Command] =>

    /**
     * A slick implementation of the 'Command' table that defines
     * the columns and conversion to/from Scala beans.
     */
    class Cmd(tag: Tag) extends Table[Command](tag, "command") {
      def id = column[Long]("frame_id", O.PrimaryKey, O.AutoInc)

      def name = column[String]("name")

      def arguments = column[String]("arguments")

      def error = column[Option[String]]("error")

      def complete = column[Boolean]("complete")
      def result = column[String]("result")

      def * = (id, name, arguments, error, complete, result) <>
        ((t: (Long, String, String, Option[String], Boolean, String)) => t match {
          case (i: Long, n: String, s: String, e: Option[String], c: Boolean, r: String) =>
            Command.tupled((i, n, JsonParser(s).convertTo[Option[JsObject]],
              e.map(err => JsonParser(err).convertTo[Error]), c, JsonParser(r).convertTo[Option[JsObject]]))
        },
          (f: Command) => Command.unapply(f) map {
            case (i, n, s, e, c, r) =>
              (i, n, s.toJson.prettyPrint, e.map(_.toJson.prettyPrint), c, r.toJson.prettyPrint)
          })
    }

    val commands = TableQuery[Cmd]

    protected val commandsAutoInc = commands returning commands.map(_.id) into {
      case (f, id) => f.copy(id = id)
    }

    // TODO: why do we need this method?
    def _insertCommand(command: CommandTemplate)(implicit session: Session) = {
      val c = Command(0, command.name, command.arguments, None, false)
      commandsAutoInc.insert(c)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      commands.where(_.id === id).mutate(f => f.delete())
    }

    override def update(command: Command)(implicit session: Session): Try[Command] = Try {
      val updated = commands.where(_.id === command.id).update(command)
      // TODO: why don't we return updated?
      command
    }

    override def insert(command: CommandTemplate)(implicit session: Session): Try[Command] = Try {
      _insertCommand(command)(session)
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[Command] = {
      commands.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[Command] = {
      commands.where(_.id === id).firstOption
    }

    /** execute DDL to create the underlying table */
    def create(implicit session: Session) = {
      commands.ddl.create
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

    class SlickGraph(tag: Tag) extends Table[Graph](tag, "graph") {
      def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

      def name = column[String]("name")

      def * = (id, name) <>
        ((t: (Long, String)) => t match {
          case (id: Long, name: String) => Graph.tupled((id, name))
        },
          (graph: Graph) => Graph.unapply(graph) map { case (i, n) => (i, n) })
    }

    val graphs = TableQuery[SlickGraph]

    protected val graphsAutoInc = graphs returning graphs.map(_.id) into { case (graph, id) => graph.copy(id = id) }

    def _insertGraph(graph: GraphTemplate)(implicit session: Session) = {
      val g = Graph(1, graph.name)
      graphsAutoInc.insert(g)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      graphs.where(_.id === id).mutate(f => f.delete())
    }

    override def update(graph: Graph)(implicit session: Session): Try[Graph] = Try {
      graphs.where(_.id === graph.id).update(graph)
      graph
    }

    override def insert(graph: GraphTemplate)(implicit session: Session): Try[Graph] = Try {
      _insertGraph(graph)(session)
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[Graph] = {
      graphs.drop(offset).take(count).list
    }

    override def lookup(id: Long)(implicit session: Session): Option[Graph] = {
      graphs.where(_.id === id).firstOption
    }

    def create(implicit session: Session) = {
      graphs.ddl.create
    }

  }
}
