package com.intel.intelanalytics.service.v1.viewmodels

import spray.json.{ JsValue }

/**
 * A value that will be inserted into the result section of a GetQuery object
 *
 * @param data Actual requested Values
 * @param partition Partition data came from
 * @param totalPartitions Total partitions for the data source
 */
case class GetQueryPartition(data: Option[List[JsValue]], partition: Option[Long], totalPartitions: Option[Long])
