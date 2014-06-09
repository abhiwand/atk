package com.intel.intelanalytics.domain.schema

import com.intel.intelanalytics.domain.schema.DataTypes.DataType

object SchemaUtil {
  /**
   * Resolve naming conflicts when both left and right side of join operation have same column names
   * @param leftColumns columns for the left side of join operation
   * @param rightColumns columns for the right side of join operation
   * @return
   */
  def resolveSchemaNamingConflicts(leftColumns: List[(String, DataType)], rightColumns: List[(String, DataType)]): List[(String, DataType)] = {

    val funcAppendLetterForConflictingNames = (left: List[(String, DataType)], right: List[(String, DataType)], appendLetter: String) => {

      var leftColumnNames = left.map(r => r._1)

      left.map(r =>
        if (right.map(i => i._1).contains(r._1)) {
          var name = r._1 + "_" + appendLetter
          while (leftColumnNames.contains(name)) {
            name = name + "_" + appendLetter
          }
          leftColumnNames = leftColumnNames ++ List(name)
          (name, r._2)
        }
        else
          r
      )
    }

    val left = funcAppendLetterForConflictingNames(leftColumns, rightColumns, "L")
    val right = funcAppendLetterForConflictingNames(rightColumns, leftColumns, "R")

    left ++ right
  }
}
