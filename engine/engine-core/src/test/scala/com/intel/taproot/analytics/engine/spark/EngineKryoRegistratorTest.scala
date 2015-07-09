/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.spark

import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.mock.MockitoSugar
import com.esotericsoftware.kryo.Kryo
import org.mockito.Mockito._
import com.intel.taproot.analytics.engine.Rows.Row
import com.intel.taproot.analytics.engine.spark.frame.LegacyFrameRdd

class EngineKryoRegistratorTest extends WordSpec with Matchers with MockitoSugar {

  "EngineKryoRegistrator" should {

    "register expected classes" in {

      val kryo = mock[Kryo]

      // invoke method under test
      new EngineKryoRegistrator().registerClasses(kryo)

      verify(kryo).register(classOf[Row])
      verify(kryo).register(classOf[LegacyFrameRdd])
    }

  }
}