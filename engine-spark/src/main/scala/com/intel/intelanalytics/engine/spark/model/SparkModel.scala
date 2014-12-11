package com.intel.intelanalytics.engine.spark.model

import com.intel.intelanalytics.domain.model.{ ModelMeta, Model }
import com.intel.intelanalytics.domain.HasData
import com.intel.intelanalytics.engine.spark.frame.FrameRDD

class SparkModel(model: Model)
    extends ModelMeta(model)
    with HasData {

  /**
   * Returns a copy with the given data instead of the current data
   */
  def withData(): SparkModel = new SparkModel(this.meta)

  type Data = FrameRDD // bogus, Model has no data RDD

  val data = null
}
