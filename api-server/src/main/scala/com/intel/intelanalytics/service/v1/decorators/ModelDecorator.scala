package com.intel.intelanalytics.service.v1.decorators

import com.intel.intelanalytics.domain.model.Model
import com.intel.intelanalytics.service.v1.viewmodels.{ RelLink, GetModels, GetModel }

object ModelDecorator extends EntityDecorator[Model, GetModels, GetModel] {

  override def decorateEntity(uri: String, links: Iterable[RelLink], entity: Model): GetModel = {

    GetModel(id = entity.id, ia_uri = entity.uri, name = entity.name, links.toList)
  }

  /**
   * Decorate a list of entities (like you would want in "GET /entities")
   *
   * @param uri the base URI, for this type of entity "../entities"
   * @param entities the list of entities to decorate
   * @return the View/Model
   */
  override def decorateForIndex(uri: String, entities: Seq[Model]): List[GetModels] = {
    entities.map(model => new GetModels(id = model.id,
      name = model.name,
      url = uri + "/" + model.id)).toList
  }
}
