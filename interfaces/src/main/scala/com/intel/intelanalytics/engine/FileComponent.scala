package com.intel.intelanalytics.engine

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
