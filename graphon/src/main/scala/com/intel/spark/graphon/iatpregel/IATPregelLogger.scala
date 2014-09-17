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
 * @tparam VD Class of the vertex data.
 * @tparam VIS Class of the vertex initial state.
 * @tparam ED Class of the edge data.
 * @tparam EIS Class of the edge initial state.
 * @tparam SSS Class of the super-step state.
 */
case class IATPregelLogger[VD: ClassTag, VIS: ClassTag, ED: ClassTag, EIS: ClassTag, SSS: ClassTag]
(vertexDataToInitialStatus: VD => VIS,
 vertexInitialStatusCombiner: (VIS, VIS) => VIS,
 edgeDataToInitialStatus: ED => EIS,
 edgeInitialStatusCombiner: (EIS, EIS) => EIS,
 generateInitialReport: (VIS, EIS) => String,
 accumulateStepStatus: (SSS, SSS) => SSS,
 convertStateToStatus: VD => SSS,
 generatePerStepReport: (SSS, Int) => String)
