package com.intel.graphbuilder.parser.rule

import RuleParserDSL._
import com.intel.graphbuilder.parser.InputRow
import com.intel.graphbuilder.parser.InputSchema
import org.specs2.mutable.Specification


class PropertyParserSpec extends Specification {

  // setup data
  val columnNames = List("id", "name", "age", "managerId", "emptyColumn", "noneColumn")
  val rowValues = List("0001", "Joe", 30, "0004", "", None)

  // setup parser dependencies
  val inputSchema = new InputSchema(columnNames, null)
  val inputRow = new InputRow(inputSchema, rowValues)

  "PropertyParser" should {

    "copy the parsed String value into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("name", column("id")))
      val property = parser.parse(inputRow)
      property.value mustEqual "0001"
    }

    "copy the parsed Int value into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("age", column("age")))
      val property = parser.parse(inputRow)
      property.value mustEqual 30
    }

    "copy the constant name into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("myname", column("id")))
      val property = parser.parse(inputRow)
      property.key mustEqual "myname"
    }

    "support parsing both the key and value from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule(column("name"), column("id")))
      val property = parser.parse(inputRow)
      property.key mustEqual "Joe"
      property.value mustEqual "0001"
    }

    "support key and values defined as constants (neither parsed from input)" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("anyName", "anyValue"))
      val property = parser.parse(inputRow)
      property.key mustEqual "anyName"
      property.value mustEqual "anyValue"
    }

    "support adding labels to vertex id's parsed from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("gbId", constant("USER.") + column("id")))
      val property = parser.parse(inputRow)
      property.key mustEqual "gbId"
      property.value mustEqual "USER.0001"
    }

    "support parsing complex compound keys and values from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule(constant("keyLabel.") + column("name") + "-" + column("id"), constant("mylabel-") + column("id")))
      val property = parser.parse(inputRow)
      property.key mustEqual "keyLabel.Joe-0001"
      property.value mustEqual "mylabel-0001"
    }

  }

  "PropertyListParser" should {

    "parse 1 property when 1 of 1 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: Nil)
      parser.parse(inputRow) must have size 1
    }

    "parse 2 properties when 2 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: new PropertyRule("managerId", column("managerId")) :: Nil)
      parser.parse(inputRow) must have size 2
    }

    "parse 1 property when 1 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: new PropertyRule("none", column("noneColumn")) :: Nil)
      parser.parse(inputRow) must have size 1
    }

    "parse 0 properties when 0 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("empty", column("emptyColumn")) :: new PropertyRule("none", column("noneColumn")) :: Nil)
      parser.parse(inputRow) must have size 0
    }

    "parse 0 properties when there are no rules" in {
      val parser = new PropertyRuleParser(Nil)
      parser.parse(inputRow) must have size 0
    }

  }
}
