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

"""
intelanalytics package init, public API
"""
import sys
if not sys.version_info[:2] == (2, 7):
    raise EnvironmentError("Python 2.7 is required for intelanalytics.  Detected version: %s.%s.%s" % tuple(sys.version_info[:3]))
del sys

from intelanalytics.core.loggers import loggers
from intelanalytics.core.iatypes import *
from intelanalytics.core.aggregation import agg
from intelanalytics.core.errorhandle import errors
from intelanalytics.core.files import CsvFile, LineFile, JsonFile, MultiLineFile, XmlFile, HiveQuery
from intelanalytics.core.iapandas import Pandas
from intelanalytics.rest.udfdepends import udf
from intelanalytics.core.frame import Frame, VertexFrame
from intelanalytics.core.graph import Graph, TitanGraph, VertexRule, EdgeRule
from intelanalytics.core.model import _BaseModel

from intelanalytics.rest.iaserver import server
connect = server.connect


try:
    from intelanalytics.core.docstubs2 import *
except Exception as e:
    errors._doc_stubs = e
    del e

# do api_globals last because other imports may have added to the api_globals


def _refresh_api_namespace():
    from intelanalytics.core.api import api_globals
    for item in api_globals:
        globals()[item.__name__] = item
    del api_globals

_refresh_api_namespace()


def _get_api_info():
    """Gets the set of all the command full names in the API"""
    from intelanalytics.meta.installapi import ApiInfo
    import sys
    return ApiInfo(sys.modules[__name__])


def _walk_api(cls_function, attr_function, include_init=False):
    """Walks the installed API and runs the given functions for class and attributes in the API"""
    from intelanalytics.meta.installapi import walk_api
    import sys
    return walk_api(sys.modules[__name__], cls_function, attr_function, include_init=include_init)


# Autoconnect if env says so.  This is NOT standard usage, but needed when
# an 'import intelanalytics' really needs to get EVERYTHING, like
# when generating documentation.  Requires that the server is already running
import os
autoconnect =  os.getenv('INTELANALYTICS_AUTOCONNECT')
#print "autoconnect=" + str(autoconnect) + " of type %s" % type(autoconnect)
if autoconnect is not None and autoconnect.lower() not in [None, '', '0', 'false']:
    print "$INTELANALYTICS_AUTOCONNECT=%s, trying to connect to IntelAnalytics..." % autoconnect
    connect()
del os
del autoconnect
