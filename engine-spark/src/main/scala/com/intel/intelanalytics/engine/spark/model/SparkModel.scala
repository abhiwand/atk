package com.intel.intelanalytics.engine.spark.model

import com.intel.intelanalytics.domain.model.{ ModelMeta, ModelEntity }
import com.intel.intelanalytics.domain.HasData
import org.apache.spark.frame.FrameRDD

class SparkModel(model: ModelEntity)
    extends ModelMeta(model)
    with HasData {

  /**
   * Returns a copy with the given data instead of the current data
   */
  def withData(): SparkModel = new SparkModel(this.meta)

  type Data = FrameRDD // bogus, Model has no data RDD

  val data = null
}
