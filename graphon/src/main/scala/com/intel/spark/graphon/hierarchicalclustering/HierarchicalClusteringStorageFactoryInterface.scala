package com.intel.spark.graphon.hierarchicalclustering

import java.io.Serializable

trait HierarchicalClusteringStorageFactoryInterface extends Serializable {

  def newStorage(): HierarchicalClusteringStorageInterface
}