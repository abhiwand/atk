#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
taprootanalytics package init, public API
"""
import sys
if not sys.version_info[:2] == (2, 7):
    raise EnvironmentError("Python 2.7 required.  Detected version: %s.%s.%s" % tuple(sys.version_info[:3]))
del sys

from taprootanalytics.core.loggers import loggers
from taprootanalytics.core.iatypes import *
from taprootanalytics.core.aggregation import agg
from taprootanalytics.core.errorhandle import errors
from taprootanalytics.core.files import CsvFile, LineFile, JsonFile, MultiLineFile, XmlFile, HiveQuery
from taprootanalytics.core.iapandas import Pandas
from taprootanalytics.rest.udfdepends import udf
from taprootanalytics.core.frame import Frame, VertexFrame
from taprootanalytics.core.graph import Graph, TitanGraph, VertexRule, EdgeRule
from taprootanalytics.core.model import _BaseModel

from taprootanalytics.rest.iaserver import server
connect = server.connect


try:
    from taprootanalytics.core.docstubs2 import *
except Exception as e:
    errors._doc_stubs = e
    del e

# do api_globals last because other imports may have added to the api_globals


def _refresh_api_namespace():
    from taprootanalytics.core.api import api_globals
    for item in api_globals:
        globals()[item.__name__] = item
    del api_globals

_refresh_api_namespace()


def _get_api_info():
    """Gets the set of all the command full names in the API"""
    from taprootanalytics.meta.installapi import ApiInfo
    import sys
    return ApiInfo(sys.modules[__name__])


def _walk_api(cls_function, attr_function, include_init=False):
    """Walks the installed API and runs the given functions for class and attributes in the API"""
    from taprootanalytics.meta.installapi import walk_api
    import sys
    return walk_api(sys.modules[__name__], cls_function, attr_function, include_init=include_init)


from taprootanalytics.core.api import api_status
from taprootanalytics.rest.iaserver import create_credentials_file

build_id = None  # This client build ID value is auto-filled during packaging.  Set to None to disable check with server
