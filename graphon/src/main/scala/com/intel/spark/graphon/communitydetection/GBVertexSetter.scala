package com.intel.spark.graphon.communitydetection

import com.intel.graphbuilder.elements.{Vertex => GBVertex, Property}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader


class GBVertexSetter(gbVertexList: RDD[GBVertex], vertexCommunitySet: RDD[(Long, Set[Long])]) extends Serializable {

  def setVertex(communityPropertyDefaultLabel: String): RDD[GBVertex] = {

    //    Define an empty set of Long
    val emptySet: Set[Long] = Set()

    //    Map the GB Vertex IDs to key-value pairs where the key is the GB Vertex ID set and the value is the emptySet.
    //    The empty set will be considered as the empty communities for the vetices that don't belong to any communities.
    val gbVertexIDEmptySetPairs: RDD[(Long, Set[Long])] = gbVertexList
      .map((v: GBVertex) => v.physicalId.asInstanceOf[Long])
      .map(id => (id, emptySet))

    //    Combine the vertices having communities with the vertices having no communities together, to have
    //    complete list of vertices
    val gbVertexCombinedWithEmptyCommunity: RDD[(Long, Set[Long])] =
      gbVertexIDEmptySetPairs.union(vertexCommunitySet).combineByKey(
        (x => x),
        ({ case (x, y) => y.union(x) }),
        ({ case (x, y) => y.union(x) }))

    val gbVertexIDOldPropertiesPairs: RDD[(Long, Seq[Property])] = gbVertexList.map({
      case GBVertex(physicalId, gbId, properties) => (physicalId.asInstanceOf[Long], properties)
    })
    //  To create a GB Vertex from the (Long, Set[Long]) pair in vertexAndCommunitySet.
    //  The ID (the long) is both the physical ID and the GB ID. Since we need a GB ID to
    //  create the vertex, we need to create some default property (TitanReader.TITAN_READER_DEFAULT_GB_ID)
    //  and stick the physical ID in there as well (so the property list will have two entries:
    //  the GB ID and the community list)
    //  The label of the communitySet property is defined as communityPropertyDefaultLabel

    //  Converted communitySet from scala Set to Java.util.Set using .asJava decorator method of JavaConverters.
    //  We need to use Java objects while actually storing to Titan using GraphBuilder.

    val gbVertexIDCommunityPropertyPairs: RDD[(Long, Seq[Property])] = gbVertexCombinedWithEmptyCommunity.map({
      case (vertexId, communitySet) => (vertexId,
        Seq(Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(communitySet))))
    })

    val gbVertexIDOldPropertiesCogroupCommunityProperty: RDD[(Long, (Seq[Seq[Property]], Seq[Seq[Property]]))] = gbVertexIDOldPropertiesPairs.cogroup(gbVertexIDCommunityPropertyPairs)

    val gbVertexIDNewProperties: RDD[(Long, Seq[Property])] = gbVertexIDOldPropertiesCogroupCommunityProperty
      .map({ case(vertexId, (Seq(seqOfOldProperties),Seq(seqOfCommunityProperty))) => (vertexId, (seqOfOldProperties,seqOfCommunityProperty) ) })
      .map({ case(vertexId, (seqOfOldProperties,seqOfCommunityProperty)) => (vertexId, seqOfOldProperties ++ seqOfCommunityProperty) })

    val updatedGBVertices: RDD[GBVertex] = gbVertexIDNewProperties.map({
      case(vertexId, newSeqOfProperties) => GBVertex(
        vertexId,
        Property(TitanReader.TITAN_READER_DEFAULT_GB_ID, vertexId),
        newSeqOfProperties)
    })
    updatedGBVertices
  }





}
