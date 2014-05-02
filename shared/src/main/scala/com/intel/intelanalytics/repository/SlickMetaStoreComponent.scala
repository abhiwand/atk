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
import com.intel.intelanalytics.domain.{User, DataFrame, Schema}
import scala.util.Try
import spray.json._
import scala.slick.driver.JdbcProfile

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
        frames.ddl.create
        users.ddl.create
      }
    }

    override lazy val frameRepo: Repository[Session, DataFrame] = new SlickFrameRepository
    override lazy val userRepo: Repository[Session, User] = new SlickUserRepository

    override def withSession[T](name: String)(f: (Session) => T): T = {
      withContext(name) {
        database.withSession(f)
      }
    }
  }

  class UserEntity(tag: Tag) extends Table[User](tag, "users") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def api_key = column[String]("api_key")

    def * = (id, api_key) <>
      ((t: (Long,String)) => t match {case (i:Long, n:String) => User.tupled((i, n))},
        (f: User) => User.unapply(f) map {case (i,n) => (i,n)})
  }

  class Frame(tag: Tag) extends Table[DataFrame](tag, "frame") {
    def id = column[Long]("frame_id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def schema = column[String]("schema")

    def * = (id, name, schema) <>
      ((t: (Long,String,String)) => t match {case (i:Long, n:String, s:String) => DataFrame.tupled((i, n, JsonParser(s).convertTo[Schema]))},
        (f: DataFrame) => DataFrame.unapply(f) map {case (i,n,s) => (i,n,s.toJson.prettyPrint)})
  }

  /* CRUD for users */
  val users = TableQuery[UserEntity]

  protected val usersAutoInc = users returning users.map(_.id) into { case (c, id) => c.copy(id = id)}

  class SlickUserRepository extends Repository[Session, User] with EventLogging {
    this: Repository[Session, User] =>

    override def insert(new_user: User) (implicit session: Session): Try[User] = Try {
      usersAutoInc.insert(new_user)
    }

    override def lookup(id: Long) (implicit session: Session): Option[User] = {
      users.where(_.id === id).firstOption
    }

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      users.where(_.id === id).mutate(c => c.delete())
    }

    override def update(c: User) (implicit session: Session): Try[User] = Try {
      users.where(_.id === c.id).update(c)
      c
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount) (implicit session: Session): Seq[User] = {
      users.drop(offset).take(count).list
    }

    override  def retrieveByColumnValue(colName: String, value: String) (implicit session: Session): List[User] = {
      users.filter(_.column[String](colName) === value).list
    }
  }

  /* CRUD for frames */
  val frames = TableQuery[Frame]

  protected val framesAutoInc = frames returning frames.map(_.id) into { case (f, id) => f.copy(id = id)}

  def _insertFrame(frame: DataFrame)(implicit session:Session) = framesAutoInc.insert(frame)


  class SlickFrameRepository extends Repository[Session, DataFrame] with EventLogging { this: Repository[Session, DataFrame] =>

    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
      frames.where(_.id === id).mutate(f => f.delete())
    }

    override def update(frame: DataFrame) (implicit session: Session): Try[DataFrame] = Try {
      frames.where(_.id === frame.id).update(frame)
      frame
    }

    override def insert(frame: DataFrame) (implicit session: Session): Try[DataFrame] = Try {
      _insertFrame(frame)(session)
    }

    override def scan(offset: Int = 0, count: Int = defaultScanCount) (implicit session: Session): Seq[DataFrame] = {
      frames.drop(offset).take(count).list
    }

    override def lookup(id: Long) (implicit session: Session): Option[DataFrame] = {
      frames.where(_.id === id).firstOption
    }

    def create(implicit session:Session) = {

    }

    override  def retrieveByColumnValue(colName: String, value: String) (implicit session: Session) : List[DataFrame] = {
      frames.filter(_.column[String](colName) === value).list
    }
  }

//  class ViewTable(profile: JdbcProfile) extends EventLogging {
//    import profile.simple._
//    import spray.json._
//    import DomainJsonProtocol._
//    class Views(tag: Tag) extends Table[View](tag, "frames") {
//      def id = column[Option[Long]]("frame_id", O.PrimaryKey, O.AutoInc)
//      def name = column[String]("name")
//      def schema = column[String]("schema")
//
//      def * = (id, name, schema) <>
//        ((t: (Option[Long],String,String)) => t match {case (i:Option[Long], n:String, s:String) => View.tupled((i, n, JsonParser(s).convertTo[Schema]))},
//          (f: DataFrame) => DataFrame.unapply(f) map {case (i,n,s) => (i,n,s.toJson.prettyPrint)})
//    }
//
//  }
//
//  val views = TableQuery[View]
//
//  protected val viewsAutoInc = views returning views.map(_.id) into { case (f, id) => f.copy(id = id)}
//
//  def insertView(view: View)(implicit session:Session) = viewsAutoInc.insert(view)
//
//  class SlickViewRepository extends Repository[Session, View] with EventLogging { this: Repository[Session, View] =>
//
//    override def delete(id: Long)(implicit session: Session): Try[Unit] = Try {
//      views.where(_.id === id).mutate(f => f.delete())
//    }
//
//    override def update(view: View) (implicit session: Session): Try[View] = Try {
//      views.where(_.id === view.id).update(view)
//      view
//    }
//
//    override def insert(view: View) (implicit session: Session): Try[View] = Try {
//      _insertFrame(view)(session)
//    }
//
//    override def scan(offset: Int = 0, count: Int = defaultScanCount) (implicit session: Session): Seq[View] = {
//      views.drop(offset).take(count).list
//    }
//
//    override def lookup(id: Long) (implicit session: Session): Option[View] = {
//      views.where(_.id === id).firstOption
//    }
//
//    def create(implicit session:Session) = {
//
//    }
//  }
}
