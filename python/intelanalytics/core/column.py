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

from intelanalytics.core.iatypes import valid_data_types


class BigColumn(object):
    """Column in a BigFrame"""
    def __init__(self, frame, name, data_type):
        self.name = name
        self.data_type = valid_data_types.get_from_type(data_type)
        self._frame = frame

    def __repr__(self):
        return '{ "name" : "%s", "data_type" : "%s" }' % (self.name, supported_types.get_type_string(self.data_type))

    def _as_json_obj(self):
        return { "name": self.name,
                 "data_type": valid_data_types.to_string(self.data_type),
                 "frame": None if not self.frame else self.frame._id}

    @property
    def frame(self):
        return self._frame

