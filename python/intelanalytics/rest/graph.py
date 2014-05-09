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
REST backend for graphs
"""
import base64
import logging
logger = logging.getLogger(__name__)
from collections import defaultdict, OrderedDict

from intelanalytics.core.column import BigColumn
from intelanalytics.core.types import *
from intelanalytics.rest.connection import rest_http


class GraphBackendRest(object):
    def get_graph_names(self):
        logger.info("REST Backend: get_graph_names")
        r = rest_http.get('graphs')
        payload = r.json()
        return [f['name'] for f in payload]

    # def get_graph(name):
    #     """Retrieves the named BigGraph object"""
    #     raise NotImplemented
    #
    # def delete_graph(name):
    #     """Deletes the graph from backing store"""
    #     raise NotImplemented

    def create(self, graph):
        logger.info("REST Backend: create graph: " + frame.name)
        # hack, steal schema early if possible...
        columns = [[n, supported_types.get_type_string(t)]
                  for n, t in frame.schema.items()]
        if not len(columns):
            try:
                if isinstance(frame._original_source,CsvFile):
                    columns = frame._original_source._schema_to_json()
            except:
                pass
        payload = {'name': frame.name, 'schema': {"columns": columns}}
        r = rest_http.post('dataframes', payload)
        logger.info("REST Backend: create response: " + r.text)
        payload = r.json()
        frame._id = payload['id']

