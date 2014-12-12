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
Serialization
"""

# WIP!

import json

class JsonEnc(json.JSONEncoder):
    _type_table = {}

    def default(self, o):
        try:
            json_obj = o._as_json_obj()
        except AttributeError:
            json_obj = o
        return json_obj


def to_json(o, encoder=JsonEnc):
    try:
        return json.dumps(o, cls=encoder)
    except Exception as e:
        return e.message

# Need a subsystem to deserialize from the REST JSON
# def from_json_str(s, encoder=JsonEnc):
#     json_obj = json.loads(s)
#     if isinstance(json_obj, list) and len(json_obj) and isinstance(json_obj[0], basestring):
#         try:
#             cls = dynamic_import(json_obj[0])
#             return cls.from_json()
#         except:
#             pass
#     return json_obj


def dynamic_import(attr_path):
    """
    Dynamically imports and returns an attribute according to the given path.
    """
    module_path, attr_name = attr_path.rsplit(".", 1)
    module = __import__(module_path, fromlist=[attr_name])
    attr = getattr(module, attr_name)
    return attr


def get_class_full_name(o):
    return o.__class__.__module__ + '.' + o.__class__.__name__
