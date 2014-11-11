##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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
"""
Post Processing of classification metrics results
"""
import pandas as pd

class ClassificationMetricsResult(object):
    """ Defines the results for binary and multi class classification metrics  """

    def __init__(self, json_result):
        self.precision = json_result['precision']
        self.f_measure = json_result['f_measure']
        self.accuracy = json_result['accuracy']
        self.recall = json_result['recall']
        if len(json_result['confusion_matrix']) > 0:
            cm_result = json_result['confusion_matrix']
            header = ['Predicted_Pos', 'Predicted_Neg']
            row_index = ['Actual_Pos', 'Actual_Neg']
            data = [(cm_result['tp'], cm_result['fn']), (cm_result['fp'],cm_result['tn'])]
            self.confusion_matrix = pd.DataFrame(data, index= row_index, columns=header)
        else:
            return "Confusion Matrix is not supported for multi class classifiers"

    def __repr__(self):
        return "Precision: {0}\nRecall: {1}\nAccuracy: {2}\nFMeasure: {3}\nConfusion Matrix: \n{4}".format(self.precision, self.recall, self.accuracy, self.f_measure, self.confusion_matrix)



