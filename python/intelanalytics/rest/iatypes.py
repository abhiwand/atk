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
Conversion functions for data type to/from REST strings
"""
from intelanalytics.core.iatypes import valid_data_types


def get_rest_str_from_data_type(data_type):
    """Returns the REST string representation for the data type"""
    return valid_data_types.to_string(data_type)  # REST accepts all the Python data types


def get_data_type_from_rest_str(rest_str):
    """Returns the data type for REST string representation"""
    if rest_str == 'string':  # string is a supported REST type; if more aliases crop up, make a table in this module
        rest_str = 'unicode'
    return valid_data_types.get_from_string(rest_str)



