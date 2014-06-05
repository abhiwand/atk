package com.intel.intelanalytics.engine

trait CommandComponent {
  def commands: CommandStorage

  trait CommandStorage {
    def lookup(id: Long): Option[Command]

    def create(frame: CommandTemplate): Command

    def scan(offset: Int, count: Int): Seq[Command]

    def start(id: Long): Unit
    def complete(id: Long, result: Try[JsObject]): Unit
  }

}
