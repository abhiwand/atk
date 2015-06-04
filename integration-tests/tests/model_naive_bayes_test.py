##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class ModelNaiveBayesTest(unittest.TestCase):
    def test_naive_bayes(self):
        print "define csv file"
        schema = [("Class", ia.int32),("Dim_1", ia.int32),("Dim_2", ia.int32),("Dim_3",ia.int32)]
        train_file = ia.CsvFile("/datasets/naivebayes_spark_data.csv", schema= schema)
        print "creating the frame"
        train_frame = ia.Frame(train_file)

        print "initializing the naivebayes model"
        n = ia.NaiveBayesModel()

        print "training the model on the frame"
        n.train(train_frame, 'Class', ['Dim_1', 'Dim_2', 'Dim_3'])

        print "predicting the class using the model and the frame"
        output = n.predict(train_frame)
        self.assertEqual(output.column_names, ['Class', 'Dim_1', 'Dim_2', 'Dim_3','predicted_class'])


if __name__ == "__main__":
    unittest.main()