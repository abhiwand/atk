package com.intel.spark.graphon.iatpregel

import scala.reflect.ClassTag

/**
 *
 * @param vertexDataToInitialStatus Converts vertex data into an initial vertex status record.
 * @param vertexInitialStatusCombiner Combines the initial statuses of multiple vertices into one.
 * @param edgeDataToInitialStatus Converts edge data into a record denoting initial edge status record.
 * @param edgeInitialStatusCombiner Combines the initial statuses of multiple edges into one.
 * @param generateInitialReport Creates the human-readable initial report on the Pregel run from the combined vertex and
 *                              edge initial statuses.
 * @param accumulateStepStatus Combines multiple per-vertex super-step status records into one status record.
 * @param convertStateToStatus Converts vertex data into super-step status record.
 * @param generatePerStepReport Convert super-status record into human readable form.
 *
 * @tparam VertexData Class of the vertex data.
 * @tparam VertexInitialState Class of the vertex initial state.
 * @tparam EdgeData Class of the edge data.
 * @tparam EdgeInitialState Class of the edge initial state.
 * @tparam SuperStepState Class of the super-step state.
 */
case class IATPregelLogger[VertexData: ClassTag,
VertexInitialState: ClassTag,
EdgeData: ClassTag,
EdgeInitialState: ClassTag,
SuperStepState: ClassTag](emptyVertexInitialStatus: VertexInitialState,
                          vertexDataToInitialStatus: VertexData => VertexInitialState,
                          vertexInitialStatusCombiner: (VertexInitialState, VertexInitialState) => VertexInitialState,
                          emptyEdgeInitialStatus: EdgeInitialState,
                          edgeDataToInitialStatus: EdgeData => EdgeInitialState,
                          edgeInitialStatusCombiner: (EdgeInitialState, EdgeInitialState) => EdgeInitialState,
                          generateInitialReport: (VertexInitialState, EdgeInitialState) => String,
                          accumulateStepStatus: (SuperStepState, SuperStepState) => SuperStepState,
                          convertStateToStatus: VertexData => SuperStepState,
                          generatePerStepReport: (SuperStepState, Int) => String)
