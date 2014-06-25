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
intelanalytics frame aggregation functions
"""


class AggregationFunctions(object):
    """
    Defines supported aggregation functions, maps them to keyword strings
    """
    avg = 'avg'
    count = 'count'
    count_distinct = 'count_distinct'
    max = 'max'
    min = 'min'
    sum = 'sum'
    var = 'var'
    stdev = 'stdev'

    def __repr__(self):
        return ", ".join([k for k in AggregationFunctions.__dict__.keys()
                          if isinstance(k, basestring) and not k.startswith("__")])

    def __contains__(self, item):
        return item in AggregationFunctions.__dict__.values()

agg = AggregationFunctions()