package com.intel.intelanalytics.domain.frame

import com.intel.intelanalytics.domain.schema.Schema

case class GetRowData(data: Array[Any], schema: Option[Schema])
