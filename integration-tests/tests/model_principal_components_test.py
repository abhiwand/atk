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

class ModelPrincipalComponentsTest(unittest.TestCase):
    def test_principal_components(self):
        print "define csv file"
        schema= [("1", ia.float64),("2", ia.float64),("3", ia.float64),("4", ia.float64),("5", ia.float64),("6", ia.float64),
                 ("7", ia.float64),("8", ia.float64),("9", ia.float64),("10", ia.float64),("11", ia.float64)]
        train_file = ia.CsvFile("/datasets/pca_10rows.csv", schema= schema)
        print "creating the frame"
        train_frame = ia.Frame(train_file)

        print "initializing the naivebayes model"
        p = ia.PrincipalComponentsModel()

        print "training the model on the frame"
        p.train(train_frame,["1","2","3","4","5","6","7","8","9","10","11"],9)

        print "predicting the class using the model and the frame"
        output = p.predict(train_frame,c=5,t_square_index=True)
        output_frame = output['output_frame']

        self.assertEqual(output_frame.column_names,['1','2','3','4','5','6','7','8','9','10','11','p_1','p_2','p_3','p_4','p_5'])
        self.assertEqual(output['t_squared_index'],20.461271109991436)


if __name__ == "__main__":
    unittest.main()