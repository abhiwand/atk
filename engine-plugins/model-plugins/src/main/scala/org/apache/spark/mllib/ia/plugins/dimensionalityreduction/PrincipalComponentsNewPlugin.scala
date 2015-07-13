package org.apache.spark.mllib.ia.plugins.dimensionalityreduction

import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.model.{ GenericNewModelArgs, ModelEntity }
import com.intel.taproot.analytics.engine.plugin.{ Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.taproot.analytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

@PluginDoc(oneLine = "Create a 'new' instance of principal component model.",
  extended = "",
  returns = "")
class PrincipalComponentsNewPlugin extends SparkCommandPlugin[GenericNewModelArgs, ModelEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:principal_components/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelEntity =
    {
      val models = engine.models
      models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:principal_components")))
    }
}

