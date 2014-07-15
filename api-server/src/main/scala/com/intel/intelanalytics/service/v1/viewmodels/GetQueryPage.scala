package com.intel.intelanalytics.service.v1.viewmodels

import spray.json.{ JsValue }

/**
 * A value that will be inserted into the result section of a GetQuery object
 *
 * @param data Actual requested Values
 * @param pages Partition data came from
 * @param totalPages Total partitions for the data source
 */
case class GetQueryPage(data: Option[List[JsValue]], pages: Option[Long], totalPages: Option[Long])
