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
REST backend for frames
"""
import requests
import json
import logging
from StringIO import StringIO
logger = logging.getLogger(__name__)
from intelanalytics.core.column import BigColumn
from intelanalytics.core.files import CsvFile
from intelanalytics.core.types import *
#from intelanalytics.rest.serialize import IAPickle

base_uri = "http://localhost:8090/v1/"
headers = {'Content-type': 'application/json', 'Accept': 'application/json'}


def post(url_arg_str, payload):
    data = (json.dumps(payload))
    #print data
    r = requests.post(base_uri + url_arg_str,
                      data=data,
                      headers=headers)
    return r


class FrameBackendREST(object):
    """REST plumbing for BigFrame"""

    def get_frame_names(self):
        logger.info("REST Backend: get_frame_names")
        r = requests.get(base_uri + 'dataframes')
        logger.info("REST Backend: get_frame_names response: " + r.text)
        payload = r.json()
        return [f['name'] for f in payload]

    def create(self, frame):
        logger.info("REST Backend: create: " + frame.name)
        payload = {'name': frame.name,
                   'schema': {"columns":
                                  [[n, supported_types.get_type_string(t)]
                                   for n, t in frame.schema.items()]}}
        r = post('dataframes', payload)
        logger.info("REST Backend: create response: " + r.text)
        payload = r.json()
        frame._id = payload['id']

    def append(self, frame, data):
        logger.info("REST Backend: Appending data to frame {0}: {1}".format(repr(frame), repr(data)))
        # for now, many data sources requires many calls to append
        if isinstance(data, list):
            for d in data:
                self.append(frame, d)
            return

        # Serialize the data source
        #  data.to_json()
        #  call REST append on the frame
        #requests.post(url, data.to_json())
        base_uri = 'http://127.0.0.1:8080/v1' # TODO make base_uri configurable
        uri = base_uri +"/dataframes/1/transforms"
        payload = {'name': 'load', 'language': 'builtin', 'arguments': {'source': data.file_name, 'separator': data.delimiter, 'skipRows': 1}}
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

        r = requests.post(uri, data=json.dumps(payload), headers = headers)
        logger.info("Response from REST server {0}".format(r.text))

        if isinstance(data, CsvFile):
            # update the Python object (set the columns)
            # todo - this info should come back from the engine
            for name, data_type in data.fields:
                if data_type is not ignore:
                    frame._columns[name] = BigColumn(name, data_type)
        else:
            raise TypeError("Unsupported append data type "
                            + data.__class__.__name__)

    def filter(self, frame, predicate):
        # payload = StringIO()
        # pickler = IAPickle(file)
        # pickler.dump(predicate)
        # Does payload have any other header/content apart from the serialized predicate for REST Server to parse?

        # pickle predicate in a payload
        # requests.post(url, payload)

        raise NotImplementedError

    def take(self, frame, n, offset):
        cmd = 'dataframes/1/data?offset={1}&count={0}'.format(n, offset)
        r = requests.get(base_uri + cmd)
        return r.json()

    def delete_frame(self, frame):
        logger.info("REST Backend: Delete frame {0}".format(repr(frame)))
        r = requests.delete(base_uri + "dataframes/" + str(frame._id))
        return r


