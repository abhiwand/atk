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
intelanalytics package init, public API
"""
import sys
if not sys.version_info[:2] == (2, 7):
    raise EnvironmentError("Python 2.7 is required for intelanalytics.  Detected version: %s.%s.%s" % tuple(sys.version_info[:3]))

from intelanalytics.core.loggers import loggers
from intelanalytics.core.iatypes import *
from intelanalytics.core.aggregation import agg
from intelanalytics.core.errorhandle import errors

try:
    from intelanalytics.core.docstubs import *
except Exception as e:
    errors._doc_stubs = e
    del e

from intelanalytics.core.files import CsvFile
from intelanalytics.core.frame import Frame, VertexFrame
from intelanalytics.core.graph import TitanGraph, VertexRule, EdgeRule
from intelanalytics.rest.connection import server
connect = server.connect


# do api_globals last because other imports may have added to the api_globals

def _refresh_api_namespace():
    from intelanalytics.core.api import api_globals
    for item in api_globals:
        globals()[item.__name__] = item
    del api_globals

_refresh_api_namespace()

