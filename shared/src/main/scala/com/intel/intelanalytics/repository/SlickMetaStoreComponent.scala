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

  class SlickMetaStore extends MetaStore with EventLogging {
    type Session = msc.Session
    override def create(): Unit = {
      withSession("Creating tables") { implicit session =>
        info("creating")
        frameRepo.asInstanceOf[SlickFrameRepository].create
        commandRepo.asInstanceOf[SlickCommandRepository].create
        info("tables created")
      }
    }

    override lazy val frameRepo: Repository[Session, DataFrameTemplate, DataFrame] = new SlickFrameRepository

    override lazy val commandRepo: Repository[Session, CommandTemplate, Command] = new SlickCommandRepository

    override def withSession[T](name: String)(f: (Session) => T): T = {
      withContext(name) {
        database.withSession(f)
      }
    }
  }



  class SlickFrameRepository extends Repository[Session, DataFrameTemplate, DataFrame]
                              with EventLogging { this: Repository[Session, DataFrameTemplate, DataFrame] =>

    class Frame(tag: Tag) extends Table[DataFrame](tag, "frame") {
      def id = column[Long]("frame_id", O.PrimaryKey, O.AutoInc)
      def name = column[String]("name")
      def schema = column[String]("schema")

      def * = (id, name, schema) <>
        ((t: (Long,String,String)) => t match {
          case (i:Long, n:String, s:String) => DataFrame.tupled((i, n, JsonParser(s).convertTo[Schema]))
        },
          (f: DataFrame) => DataFrame.unapply(f) map {case (i,n,s) => (i,n,s.toJson.prettyPrint)})
    }

    val frames = TableQuery[Frame]

    protected val framesAutoInc = frames returning frames.map(_.id) into { case (f, id) => f.copy(id = id)}

    def _insertFrame(frame: DataFrameTemplate)(implicit session:Session) = {
      val f = DataFrame(0, frame.name, frame.schema)
      framesAutoInc.insert(f)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      frames.where(_.id === id).mutate(f => f.delete())
    }

    override def update(frame: DataFrame) (implicit session: Session): Try[DataFrame] = Try {
      frames.where(_.id === frame.id).update(frame)
      frame
    }

    override def insert(frame: DataFrameTemplate) (implicit session: Session): Try[DataFrame] = Try {
      _insertFrame(frame)(session)
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount) (implicit session: Session): Seq[DataFrame] = {
      frames.drop(offset).take(count).list
    }

    override def lookup(id: Long) (implicit session: Session): Option[DataFrame] = {
      frames.where(_.id === id).firstOption
    }

    def create(implicit session:Session) = {
      frames.ddl.create
    }
  }

  class SlickCommandRepository extends Repository[Session, CommandTemplate, Command]
                                with EventLogging { this: Repository[Session, CommandTemplate, Command] =>

    class Cmd(tag: Tag) extends Table[Command](tag, "command") {
      def id = column[Long]("frame_id", O.PrimaryKey, O.AutoInc)
      def name = column[String]("name")
      def arguments = column[String]("arguments")
      def error = column[Option[String]]("error")
      def complete = column[Boolean]("complete")

      def * = (id, name, arguments, error, complete) <>
        ((t: (Long,String,String,Option[String],Boolean)) => t match {
          case (i:Long, n:String, s:String, e:Option[String], c:Boolean) =>
            Command.tupled((i, n, JsonParser(s).convertTo[Option[JsObject]],
              e.map(err => JsonParser(err).convertTo[Error]), c))
        },
          (f: Command) => Command.unapply(f) map {case (i,n,s,e,c) =>
            (i,n,s.toJson.prettyPrint,e.map(_.toJson.prettyPrint),c)})
    }

    val commands = TableQuery[Cmd]

    protected val commandsAutoInc = commands returning commands.map(_.id) into { case (f, id) => f.copy(id = id)}

    def _insertCommand(command: CommandTemplate)(implicit session:Session) = {
      val c = Command(0, command.name, command.arguments, None, false)
      commandsAutoInc.insert(c)
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      commands.where(_.id === id).mutate(f => f.delete())
    }

    override def update(command: Command) (implicit session: Session): Try[Command] = Try {
      val updated = commands.where(_.id === command.id).update(command)
      command
    }

    override def insert(command: CommandTemplate) (implicit session: Session): Try[Command] = Try {
      _insertCommand(command)(session)
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount) (implicit session: Session): Seq[Command] = {
      commands.drop(offset).take(count).list
    }

    override def lookup(id: Long) (implicit session: Session): Option[Command] = {
      commands.where(_.id === id).firstOption
    }

    def create(implicit session:Session) = {
      commands.ddl.create
    }
  }
}
