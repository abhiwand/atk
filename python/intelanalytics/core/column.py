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

from intelanalytics.core.types import unknown, supported_types

class BigColumn(object):
    """Column in a BigFrame"""
    def __init__(self, name, data_type=unknown):
        self.name = name
        if data_type is not unknown:
            supported_types.validate_is_supported_type(data_type)
        self.data_type = data_type
        self.frame = None  # may want to make a property, help prevent setting this externally

    def as_json_obj(self):
        frame_id = 1234  # todo - bologna for now
        return ["column", { "name": self.name, "frame": frame_id}]

