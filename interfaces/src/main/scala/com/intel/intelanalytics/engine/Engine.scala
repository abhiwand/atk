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
import com.intel.intelanalytics.domain.{Schema, DataFrame}
import scala.concurrent.Future
import java.io.{OutputStream, InputStream}

object Rows {
  type Row = Seq[Any] //TODO: Can we constrain this better?
  trait RowSource {
    def schema: Schema
    def rows: Iterable[Row]
  }

}

trait Functional {
  def language: String
  def definition: String
}

case class RowFunction[T](language: String, definition: String) extends Functional
case class Builtin(name: String) extends Functional { def language = "builtin"; def definition = name }


trait FrameComponent {
  def frames: FrameStorage

  type Frame = DataFrame
  type Row <: Rows.Row
  type View <: DataView

  trait Column[T] {
    def name : String
  }

  trait DataView {

  }

  sealed abstract class Alteration { }

  case class AddColumn[T](name: String, value: Option[T], generator: Row => T) extends Alteration
  case class RemoveColumn[T](name: String) extends Alteration

  trait FrameStorage {
    def compile[T](func: RowFunction[T]): Row => T
    def create(frame: Frame): Frame
    def scan(frame: Frame): Iterable[Row]
    def addColumn[T](frame: Frame, column: Column[T], generatedBy: Row => T): Unit
    def addColumnWithValue[T](frame: Frame, column: Column[T], default: T): Unit
    def removeColumn(frame: Frame): Unit
    def removeRows(frame: Frame, predicate: Row => Boolean)
    def appendRows(startWith: Frame, append: Iterable[Row])
    def drop(frame: Frame)
  }

  trait ViewStorage {
    def viewOfFrame(frame: Frame): View
    def create(view: View): Unit
    def scan(view: View): Iterable[Row]
    def addColumn[T](view: View, column: Column[T], generatedBy: Row => T): View
    def addColumnWithValue[T](view: View, column: Column[T], default: T): View
    def removeColumn(view: View): View
    def removeRows(view: View, predicate: Row => Boolean): View
    def appendRows(startWith: View, append: View) : View
  }
}

trait FileComponent {
  def files: FileStorage

  sealed abstract class Entry(path: Path) {  }

  case class File(path: Path, size: Long = 0) extends Entry(path)

  case class Directory(path: Path) extends Entry(path)

  trait FileStorage {
    def create(entry: Entry)
    def delete(path: Path)
    def getMetaData(path: Path): Option[Entry]
    def move(source: Path, destination: Path)
    def copy(source: Path, destination: Path)
    def read(source: File) : InputStream
    def list(source: Directory): Seq[Entry]
    def readRows(source: File, rowGenerator: InputStream => Rows.RowSource, offsetBytes: Long = 0, readBytes: Long = -1)
    def write(sink: File, append: Boolean = false): OutputStream
  }
}

trait EngineComponent { this: EngineComponent with FrameComponent
                                              with FileComponent =>

  type Identifier = Long //TODO: make more generic?

  def engine: Engine
  //TODO: make these all use Try instead?
  trait Engine {
    def getFrame(id: Identifier) : Future[DataFrame]
    def create(frame: DataFrame): Future[DataFrame]
    def clear(frame: DataFrame) : Future[DataFrame]
    def appendFile(frame: DataFrame, file: String, parser: Functional) : Future[DataFrame]
    //def append(frame: DataFrame, rowSource: Rows.RowSource): Future[DataFrame]
    def filter(frame: DataFrame, predicate: RowFunction[Boolean]): Future[DataFrame]
    def alter(frame: DataFrame, changes: Seq[Alteration])
    def delete(frame: DataFrame): Future[Unit]
  }
}
