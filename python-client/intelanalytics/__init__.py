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
intelanalytics package init, public API
"""
import sys
if not sys.version_info[:2] == (2, 7):
    raise EnvironmentError("Python 2.7 required.  Detected version: %s.%s.%s" % tuple(sys.version_info[:3]))
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


def _dump_server_command_defs(file_name):
    """Gets command defs from server and dumps them to a file, as raw JSON"""
    from intelanalytics.meta.installapi import dump_server_command_defs_to_file
    dump_server_command_defs_to_file(server, file_name)


def _walk_api(cls_function, attr_function, include_init=False):
    """Walks the installed API and runs the given functions for class and attributes in the API"""
    from intelanalytics.meta.installapi import walk_api
    import sys
    return walk_api(sys.modules[__name__], cls_function, attr_function, include_init=include_init)


from intelanalytics.core.api import api_status
from intelanalytics.rest.iaserver import create_credentials_file

build_id = None  # This client build ID value is auto-filled during packaging.  Set to None to disable check with server
