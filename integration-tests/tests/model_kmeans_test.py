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
import taprootanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class ModelKMeansTest(unittest.TestCase):
    def testKMeans(self):
        print "define csv file"
        csv = ia.CsvFile("/datasets/KMeansTestFile.csv", schema= [('data', ia.float64),
                                                             ('name', str)], skip_header_lines=1)

        print "create frame"
        frame = ia.Frame(csv)

        print "Initializing a KMeansModel object"
        k = ia.KMeansModel(name='myKMeansModel')

        print "Training the model on the Frame"
        k.train(frame,['data'],[2.0])


if __name__ == "__main__":
    unittest.main()
