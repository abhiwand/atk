package com.intel.intelanalytics.domain.graph

/**
 * Class containing the list of Element ID names corresponding to GraphElements and the name of their id column
 */
case class ElementIDNames(elementIDNames: List[ElementIDName]) {
  require(elementIDNames != null, "element id names cannot be null")
}

/**
 * Name Value pairing of a graph element and its corresponding id column name
 * @param label label of graph element
 * @param idColumnName column name of unique identifier.
 */
case class ElementIDName(label: String, idColumnName: String) {
  require(!label.isEmpty, "label is required")
  require(!idColumnName.isEmpty, "idColumnName is required")
}
