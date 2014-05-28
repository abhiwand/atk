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

package com.intel.intelanalytics.engine

import scala.xml.persistent.CachedFileStorage
import java.nio.file.Path
import PartialFunction._
import com.intel.intelanalytics.domain._
import scala.concurrent.Future
import java.io.{ OutputStream, InputStream }
import com.intel.intelanalytics.engine.Rows.Row
import spray.json.JsObject
import scala.util.Try
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.Graph
import com.intel.intelanalytics.domain.CommandTemplate
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.domain.FrameRemoveColumn
import com.intel.intelanalytics.domain.Schema
import com.intel.intelanalytics.domain.GraphTemplate
import com.intel.intelanalytics.domain.LoadLines
import com.intel.intelanalytics.domain.Command
import com.intel.intelanalytics.domain.DataFrameTemplate
import com.intel.intelanalytics.domain.FrameAddColumn
import com.intel.intelanalytics.domain.Als

object Rows {
  type Row = Array[Any] //TODO: Can we constrain this better?
  trait RowSource {
    def schema: Schema
    def rows: Iterable[Row]
  }

}

//object EngineMessages {
//  case class AppendFile(id: Long, fileName: String, rowGenerator: Functional)
//  case class DropColumn(id: Long, name: String)
//  case class AddColumn(id: Long, name: String, map: Option[RowFunction[Any]])
//  case class DropRows(id: Long, filter: RowFunction[Boolean])
//}

sealed abstract class Alteration {}

case class AddColumn[T](name: String, value: Option[T], generator: Row => T) extends Alteration
case class RemoveColumn[T](name: String) extends Alteration

trait FrameComponent {
  import Rows.Row
  def frames: FrameStorage

  type View <: DataView

  trait Column[T] {
    def name: String
  }

  trait DataView {

  }

  trait FrameStorage {
    def lookup(id: Long): Option[DataFrame]
    def create(frame: DataFrameTemplate): DataFrame
    def addColumn[T](frame: DataFrame, column: Column[T], columnType: DataTypes.DataType): DataFrame
    def addColumnWithValue[T](frame: DataFrame, column: Column[T], default: T): Unit
    def removeColumn(frame: DataFrame, columnIndex: Seq[Int]): Unit
    def renameColumn[T](frame: DataFrame, name_pairs: Seq[(String, String)]): Unit
    def removeRows(frame: DataFrame, predicate: Row => Boolean)
    def appendRows(startWith: DataFrame, append: Iterable[Row])
    def getRows(frame: DataFrame, offset: Long, count: Int)(implicit user: UserPrincipal): Iterable[Row]
    def drop(frame: DataFrame)
  }

  trait GraphStorage {
    def lookup(id: Long): Option[Graph]
    def createGraph(graph: GraphTemplate): Graph
    def drop(graph: Graph)

  }

  //  trait ViewStorage {
  //    def viewOfFrame(frame: Frame): View
  //    def create(view: View): Unit
  //    def scan(view: View): Iterable[Row]
  //    def addColumn[T](view: View, column: Column[T], generatedBy: Row => T): View
  //    def addColumnWithValue[T](view: View, column: Column[T], default: T): View
  //    def removeColumn(view: View): View
  //    def removeRows(view: View, predicate: Row => Boolean): View
  //    def appendRows(startWith: View, append: View) : View
  //  }
}

trait FileComponent {
  def files: FileStorage

  sealed abstract class Entry(path: Path) {}

  case class File(path: Path, size: Long = 0) extends Entry(path)

  case class Directory(path: Path) extends Entry(path)

  trait FileStorage {
    def createDirectory(name: Path): Directory
    def create(name: Path)
    def delete(path: Path)
    def getMetaData(path: Path): Option[Entry]
    def move(source: Path, destination: Path)
    def copy(source: Path, destination: Path)
    def read(source: File): InputStream
    def list(source: Directory): Seq[Entry]
    def readRows(source: File, rowGenerator: InputStream => Rows.RowSource, offsetBytes: Long = 0, readBytes: Long = -1)
    def write(sink: File, append: Boolean = false): OutputStream
  }
}

trait EngineComponent {

  import Rows.Row
  type Identifier = Long //TODO: make more generic?

  def engine: Engine
  //TODO: make these all use Try instead?
  //TODO: make as many of these as possible use id instead of dataframe as the first argument?
  //TODO: distinguish between DataFrame and DataFrameSpec,
  // where the latter has no ID, and is the argument passed to create?
  trait Engine {
    def getCommands(offset: Int, count: Int): Future[Seq[Command]]
    def getCommand(id: Identifier): Future[Option[Command]]
    def getFrame(id: Identifier): Future[Option[DataFrame]]
    def getRows(id: Identifier, offset: Long, count: Int)(implicit user: UserPrincipal): Future[Iterable[Row]]
    def create(frame: DataFrameTemplate): Future[DataFrame]
    def clear(frame: DataFrame): Future[DataFrame]
    def load(arguments: LoadLines[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])
    def filter(arguments: FilterPredicate[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])
    def project(arguments: FrameProject[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])
    def renameColumn(arguments: FrameRenameColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])
    //  Should predicate be Partial[Any]  def filter(frame: DataFrame, predicate: Partial[Any])(implicit user: UserPrincipal): Future[DataFrame]
    def removeColumn(arguments: FrameRemoveColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])
    def addColumn(arguments: FrameAddColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])
    def alter(frame: DataFrame, changes: Seq[Alteration])
    def delete(frame: DataFrame): Future[Unit]
    def flattenColumn(argument: FlattenColumn[Long])(implicit user: UserPrincipal): (Command, Future[Command])
    def join(argument: FrameJoin[Long])(implicit user: UserPrincipal): (Command, Future[Command])

    def getFrames(offset: Int, count: Int)(implicit p: UserPrincipal): Future[Seq[DataFrame]]
    def shutdown: Unit
    def getGraph(id: Identifier): Future[Graph]
    def getGraphs(offset: Int, count: Int): Future[Seq[Graph]]
    def createGraph(graph: GraphTemplate): Future[Graph]
    def deleteGraph(graph: Graph): Future[Unit]
    //NOTE: we do /not/ expect to have a separate method for every single algorithm, this will move to a plugin
    //system soon
    def runAls(als: Als[Long]): (Command, Future[Command])
  }
}

trait CommandComponent {
  def commands: CommandStorage

  trait CommandStorage {
    def lookup(id: Long): Option[Command]
    def create(frame: CommandTemplate): Command
    def scan(offset: Int, count: Int): Seq[Command]
    def start(id: Long): Unit
    def complete(id: Long, result: Try[Any]): Unit
  }

}
