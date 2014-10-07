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
from intelanalytics.core.loggers import loggers
from intelanalytics.core.iatypes import *
from intelanalytics.core.aggregation import agg
from intelanalytics.core.errorhandle import errors
from intelanalytics.core.files import CsvFile
from intelanalytics.core.frame import BigFrame, Frame
from intelanalytics.core.graph import BigGraph, get_graph, get_graph_names, drop_graphs, VertexRule, EdgeRule
from intelanalytics.rest.connection import server
connect = server.connect


# do api_globals last because other imports may have added to the api_globals
from intelanalytics.core.api import api_globals


def _refresh_api_namespace():
    for item in api_globals:
        globals()[item.__name__] = item

_refresh_api_namespace()


# AUTO-CONNECT  TODO - remove
import os
if not os.getenv('INTELANALYTICS_SKIP_AUTOCONNECT', False):  # TODO - put all the env var names in one place
    from intelanalytics.core.api import FatalApiLoadError
    # Try to load the api on import...
    try:
        connect()
    except FatalApiLoadError as not_fatal_here:
        # raising just a RuntimeError here will cause the intelanalytics import to fail generally.
        raise RuntimeError("Processing the API information from the server failed: %s" % not_fatal_here.details)
    except Exception as e:
        msg = """
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

%s

Unable to connect to server during import. Resolve issues and then call
'connect()' to continue.%s
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""" % (e, '' if not type(e).__module__.startswith("requests") else """

Use 'server' to view and edit client's server settings.  For example,
'print ia.server'
""")
        import warnings
        warnings.warn(msg, RuntimeWarning)
        del e, msg
    print """
**This auto-connect feature is deprecated and soon an explicit call to
'ia.connect()' will be required before using the server features
"""
del os
