##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
from collections import defaultdict
from mock import Mock, MagicMock
from testutils import create_mock_registry


dynamic_import = Mock()
get_time_str = Mock(return_value='_test_time')
Registry = MagicMock(side_effect=create_mock_registry)

dd = defaultdict(lambda: 'mocked')


def getitem(key):
    return dd[key]


def setitem(key, value):
    dd[key] = value


def delitem(key):
    del dd[key]

global_config = MagicMock()
global_config.__getitem__.side_effect = getitem
global_config.__setitem__.side_effect = setitem
global_config.__delitem__.side_effect = delitem
global_config.verify.return_value = True
