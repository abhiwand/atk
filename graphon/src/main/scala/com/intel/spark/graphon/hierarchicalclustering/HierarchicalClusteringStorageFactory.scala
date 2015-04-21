package com.intel.spark.graphon.hierarchicalclustering

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration

case class HierarchicalClusteringStorageFactory(dbConnectionConfig: SerializableBaseConfiguration)
    extends HierarchicalClusteringStorageFactoryInterface {

  override def newStorage(): HierarchicalClusteringStorage = {
    val titanConnector = new TitanGraphConnector(dbConnectionConfig)
    val titanGraph = titanConnector.connect()
    new HierarchicalClusteringStorage(titanGraph)
  }
}