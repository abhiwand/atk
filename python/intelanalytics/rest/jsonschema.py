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
Json-Schema definitions and interactions
"""
import logging
logger = logging.getLogger(__name__)

import json

from intelanalytics.core.command import CommandDefinition, Parameter, Return, Doc, Version
from intelanalytics.core.iatypes import *
from intelanalytics.core.frame import BigFrame
from intelanalytics.core.graph import BigGraph

__all__ = ['get_command_def']


# See http://json-schema.org/documentation.html
# And source_code/interfaces/src/main/scala/com/intel/intelanalytics/schema/JsonSchema.scala

json_type_id_to_data_type  = {
    "ia:int": int32,
    "ia:long": int64,
    "ia:float": float32,
    "ia:double": float64,
}

json_str_formats_to_data_type = {
    "uri/ia-frame": BigFrame,
    "uri/ia-graph": BigGraph,
}


def get_data_type(json_schema):
    """
    Returns Python data type for type found in the topmost element of the json_schema
    """
    try:
        data_type = None
        # first try from id
        if 'id' in json_schema:
            data_type = json_type_id_to_data_type.get(json_schema['id'], None)
        # next try from type
        if not data_type:
            if 'type' not in json_schema:
                return None
            t = json_schema['type']
            if t == 'null':
                return None  # exit
            if t == 'string':
                if 'format' in json_schema:
                    data_type = json_str_formats_to_data_type.get(json_schema['format'], unicode)
                else:
                    data_type = unicode
            elif t == 'array':
                data_type = list
            elif t == 'object':
                data_type = dict  # use dict for now, TODO - add complex type support

        if not data_type:
            #data_type =  dict  # use dict for now, TODO - add complex type support
            raise ValueError("Could not determine data type from server information:\n  %s" %
                             ("\n  ".join(["%s-%s" % (k, v) for k, v in json_schema.iteritems()])))
        return data_type
    except:
        log_schema_error(json_schema)
        raise


def log_schema_error(json_schema):
    if json_schema is None:
        schema_str = "(json_schema is None!)"
    else:
        try:
            schema_str = json.dumps(json_schema, indent=2)
        except:
            schema_str = "(Unable to dump schema!)"
    logger.error("Error in json_schema:\n%s" % schema_str)


def get_doc(json_schema):
    doc = json_schema.get('doc', {})
    return Doc(doc.get('title', ''), doc.get('description', ''))


def get_parameters(argument_schema):
    """Builds list of Parameter tuples as represented by the 'argument_schema'"""
    # Note - using the common convention that "parameters" are the variables in function definitions
    # and arguments are the values being passed in.  'argument_schema' is used in the rest API however.
    parameters = []
    for name in argument_schema['order']:
        properties = argument_schema['properties'][name]
        data_type = get_data_type(properties)
        use_self = properties.get('self', False)
        optional = name not in argument_schema['required']
        default = properties.get('default', None)
        doc = get_doc(properties)
        parameters.append(Parameter(name, data_type, use_self, optional, default, doc))
    return parameters


def get_return(return_schema):
    """Returns a Return tuple according to the return_schema"""
    # Get the definition of what happens with the return  --TODO, enhance for Complex Types, etc...
    # 1. return Simple/Primitive Type
    # 2. return Frame or Graph reference
    # 3. return Complex Type
    # 4. return None  (no return value)
    data_type = get_data_type(return_schema)
    use_self = return_schema.get('self', False)
    if use_self and data_type not in [BigFrame, BigGraph]:
        raise TypeError("Error loading commands: use_self is True, but data_type is %s" % data_type)
    doc = get_doc(return_schema)
    return Return(data_type, use_self, doc)


def get_version(json_schema):   # TODO - this is first-cut, needs reqs+review
    """Returns a Version object or None"""
    v = json_schema.get('version', {})
    return Version(v.get('added', None), v.get('changed', None), v.get('deprecated', None), v.get('description', None))


def get_command_def(json_schema):
    """Returns a CommandDefinition obj according to the json schema"""
    full_name = json_schema['name']
    parameters = get_parameters(json_schema['argument_schema'])
    return_type = get_return(json_schema['return_schema'])
    version = get_version(json_schema)
    doc = get_doc(json_schema)
    return CommandDefinition(json_schema, full_name, parameters, return_type, doc, version)
