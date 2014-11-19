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
Model
"""

import logging
import json
logger = logging.getLogger(__name__)
from intelanalytics.core.api import get_api_decorator
api = get_api_decorator(logger)

from intelanalytics.core.namedobj import name_support
from intelanalytics.core.metaprog import CommandLoadable, doc_stubs_import, get_command_prefix_from_class_name
from intelanalytics.core.errorhandle import IaError
from intelanalytics.rest.connection import http

# _BaseModel
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubs_BaseModel
    doc_stubs_import.success(logger, "DocStubs_BaseModel")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubs_BaseModel", e)
    class DocStubs_BaseModel(object): pass


@api
class _BaseModel(DocStubs_BaseModel, CommandLoadable):
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
    _command_prefix = 'model'

    def __init__(self):
        self._id = 0
        CommandLoadable.__init__(self)

    def _get_model_info(self):
        response = http.get_full_uri(self._get_model_full_uri())
        return ModelInfo(response.json())

    def _get_model_full_uri(self):
        return self.rest_http.create_full_uri('models/%d' % self._id)

    def __repr__(self):
        try:
            model_info = self._get_model_info()
            return "\n".join([self.__class__.__name__, 'name =  "%s"' % (model_info.name)])
        except:
            return super(_BaseModel,self).__repr__() + " (Unable to collect metadata from server)"

    def __eq__(self, other):
        if not isinstance(other, _BaseModel):
            return False
        return self._id == other._id

# LogisticRegressionModel
# TODO - remove once metaprog can handle generating these models on the fly
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsLogisticRegressionModel
    doc_stubs_import.success(logger, "DocStubsLogisticRegressionModel")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsLogisticRegressionModel", e)
    class DocStubsLogisticRegressionModel(object): pass


@api
@name_support('model')
class LogisticRegressionModel(DocStubsLogisticRegressionModel, _BaseModel):
    """
    LogisticRegressionModel model instantiation.

    Parameters
    ----------
    name: str
        Name of the LogisticRegressionModel

    Returns
    -------
    LogisticRegressionModel object
        An object with access to the LogisticRegressionModel

    Examples
    --------
    model = ia.LogisticRegressionModel(name='LogReg')
    """
    _command_prefix = "model:logistic_regression"

    def __init__(self, source=None, name=None):
        try:
            self._id = 0
            CommandLoadable.__init__(self)
            self._create(source, name)
        except:
            error = IaError(logger)
            raise error


    def _create(self, source, name):
        if isinstance(source, dict):
            source = ModelInfo(source)
        if isinstance(source, ModelInfo):
            source.initialize_model(self)
        elif source is None:
        #if isinstance(source, Frame):
            # create
            command_prefix = get_command_prefix_from_class_name(self.__class__.__name__)
            payload = {'name': name, 'model_type': command_prefix.split(':')[-1]}
            r = http.post('models', payload)
            ModelInfo(r.json()).initialize_model(self)
        #elif source is not None:
        else:
            raise ValueError("Invalid source type %s.  Expected Frame or Model, got %s" % type(source))

        return self.name

    def _get_model_info(self):
        response = http.get_full_uri(self._get_model_full_uri())
        return ModelInfo(response.json())

    def _get_model_full_uri(self):
        return http.create_full_uri('models/%d' % self._id)

    def __repr__(self):
        try:
            model_info = self._get_model_info()
            return '%s "%s"' % (self.__class__.__name__, model_info.name)
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
        return self._payload['name']

    @property
    def ia_uri(self):
        return self._payload['ia_uri']

    @property
    def links(self):
        return self._links['links']

    def initialize_model(self, model):
        model._id = self.id_number

    def update(self,payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, model ID mismatch %d when expecting %d" \
                  % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload=payload
