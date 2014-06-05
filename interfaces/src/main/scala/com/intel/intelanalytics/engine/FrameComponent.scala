package com.intel.intelanalytics.engine

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

    def renameFrame(frame: DataFrame, newName: String): Unit

    def renameColumn(frame: DataFrame, name_pairs: Seq[(String, String)]): Unit

    def removeRows(frame: DataFrame, predicate: Row => Boolean)

    def appendRows(startWith: DataFrame, append: Iterable[Row])

    def getRows(frame: DataFrame, offset: Long, count: Int)(implicit user: UserPrincipal): Iterable[Row]

    def drop(frame: DataFrame)
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
