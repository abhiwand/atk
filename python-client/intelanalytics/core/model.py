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
Model
"""

import logging
import json
logger = logging.getLogger(__name__)
from intelanalytics.meta.clientside import *
api = get_api_decorator(logger)

from intelanalytics.meta.namedobj import name_support
from intelanalytics.meta.metaprog import CommandInstallable as CommandLoadable
from intelanalytics.meta.docstub import doc_stubs_import
from intelanalytics.rest.iaserver import server

# _BaseModel
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs1 import _DocStubs_BaseModel
    doc_stubs_import.success(logger, "_DocStubs_BaseModel")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubs_BaseModel", e)
    class _DocStubs_BaseModel(object): pass


@api
@name_support('model')
class _BaseModel(_DocStubs_BaseModel, CommandLoadable):
    """
    Class with information about a model.
    Has information needed to modify data and table structure.

    Parameters
    -----------
    name: string
        The name of the newly created model

    Returns
    -------
    Model
        An object with access to the model
    """
    _entity_type = 'model'

    def __init__(self):
        self._id = 0
        CommandLoadable.__init__(self)

    def _get_model_info(self):
        response = server.get(self._get_model_full_uri())
        return ModelInfo(response.json())

    def _get_model_full_uri(self):
        return server.create_full_uri('models/%d' % self._id)

    @staticmethod
    def _is_entity_info(obj):
        return isinstance(obj, ModelInfo)

    def __repr__(self):
        try:
            model_info = self._get_model_info()
            return "\n".join([self.__class__.__name__, 'name =  "%s"' % (model_info.name), "status = %s" % model_info.status])
        except:
            return super(_BaseModel,self).__repr__() + " (Unable to collect metadata from server)"

    def __eq__(self, other):
        if not isinstance(other, _BaseModel):
            return False
        return self._id == other._id


class ModelInfo(object):
    """
    JSON based Server description of a Model
    """
    def __init__(self, model_json_payload):
        self._payload = model_json_payload
        self._validate()

    def __repr__(self):
        return json.dumps(self._payload, indent =2, sort_keys=True)

    def __str__(self):
        return '%s "%s"' % (self.id_number, self.name)

    def _validate(self):
        try:
            assert self.id_number
        except KeyError:
            raise RuntimeError("Invalid response from server. Expected Model info.")

    @property
    def id_number(self):
        return self._payload['id']

    @property
    def name(self):
        return self._payload.get('name', None)

    @property
    def ia_uri(self):
        return self._payload['ia_uri']

    @property
    def links(self):
        return self._links['links']

    @property
    def status(self):
        return self._payload['status']

    def initialize_model(self, model):
        model._id = self.id_number

    def update(self,payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, model ID mismatch %d when expecting %d" \
                  % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload=payload
