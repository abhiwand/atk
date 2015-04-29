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

class ModelLinearRegressionTest(unittest.TestCase):
    def testLinearRegression(self):
        print "define csv file"

        csv = ia.CsvFile("/datasets/lin_reg_8.csv", schema = [("y", ia.float64),("1",ia.float64),("2",ia.ia.float64),
                                                              ("3",ia.float64),("4",ia.float64),("5",ia.float64),
                                                              ("6",ia.float64),("7",ia.float64),("8",ia.float64),
                                                              ("9",ia.float64),("10",ia.float64)],skip_header_lines=1)

        print "create frame"
        frame = ia.Frame(csv)

        print "Initializing a LinearRegressionModel object"
        k = ia.LinearRegressionModel(name='myLinearRegressionModel')

        print "Training the model on the Frame"
        k.train(frame,'y',['1','2','3','4','5','6','7','8','9','10'])

        print "Predicting on the Frame"
        m = k.predict(frame)

        self.assertEqual(m.column_names,['y','1','2','3','4','5','6','7','8','9','10','predicted_value'])


if __name__ == "__main__":
    unittest.main()
