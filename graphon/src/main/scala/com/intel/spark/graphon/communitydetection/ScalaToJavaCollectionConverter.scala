package com.intel.spark.graphon.communitydetection

import scala.collection.JavaConversions._

object ScalaToJavaCollectionConverter extends Serializable {
  def convertSet(scalaSet: Set[Long]): java.util.Set[Long] = {
    val javaSet = new java.util.HashSet[Long]()
    scalaSet.foreach(entry => javaSet.add(entry))
    javaSet
  }

}
