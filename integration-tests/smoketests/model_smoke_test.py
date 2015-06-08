#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class ModelSmokeTest(unittest.TestCase):
    """
    Smoke test basic frame operations to verify functionality that will be needed by all other tests.

    If these tests don't pass, there is no point in running other tests.

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    def test_model(self):
        print "Initialize KMeansModel object with name"
        k1 = ia.KMeansModel(name='mykMeansModel1')
        name = k1.name

        print "Initialize KMeansModel object"
        k2 = ia.KMeansModel()

        print "Initialize LogisticRegressionModel object with name"
        l1= ia.LogisticRegressionModel(name='myLogisticRegressionModel1')

        print "Initialize LogisticRegressionModel object"
        l2 = ia.LogisticRegressionModel()

        print "Initialize NaiveBayesModel object"
        n = ia.NaiveBayesModel()

if __name__ == "__main__":
    unittest.main()
