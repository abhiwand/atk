package com.intel.intelanalytics.component

import org.scalatest._
import java.net.URL

class BootSpec extends FlatSpec with Matchers {
  "getJar" should "return a single jar location" in {

    val url: URL = new URL("http://localhost/path/engine-spark.jar")
    val f = (s: String) => {
      Array(new URL("http://localhost/path/classes"), url)
    }

    Boot.getJar("engine-spark", f) shouldEqual url
  }

  "getJar" should "raise exception when no jar is found" in {

    val f = (s: String) => {
      Array(new URL("http://localhost/path/classes"))
    }

    intercept[Exception] {
      Boot.getJar("engine-spark", f)
    }
  }

}
