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
intel_analytics definitions for Data Types
"""
import numpy as np

# alias numpy and unicode
float32 = np.float32
float64 = np.float64
int32 = np.int32
int64 = np.int64
string = unicode


class _Ignore(object):
    """Ignore type used for schemas during file import"""
    pass

ignore = _Ignore


class _Unknown(object):
    """Unknown type used when type is indeterminate"""
    pass

unknown = _Unknown


class _Types(frozenset):

    _ia_types = {"bool": bool,
                 "bytearray": bytearray,
                 "dict": dict,
                 "float32": float32,
                 "float64": float64,
                 "int32": int32,
                 "int64": int64,
                 "list": list,
                 "str": str,
                 "string": string}

    _python_to_ia = {int: int64,
                     long: int64,
                     float: float64}

    def validate_is_supported_type(self, data_type):
        self.get_type_string(data_type)

    def get_type_string(self, t):
        for k, v in _Types._ia_types.items():
            if v is t:
                return k
        raise TypeError("Unsupported type " + str(t))

    def try_get_type_string(self, t):
        try:
            return self.get_type_string(t)
        except TypeError:
            return type(t).__name__

    def get_type(self, obj):
        t = type(obj)
        if t in _Types._ia_types.values():
            return t
        return _Types._python_to_ia[t]

    def try_get_type(self, obj):
        try:
            return self.get_type(obj)
        except KeyError:
            return unknown

    def get_type_from_string(self, type_string):
        return _Types._ia_types[type_string]

    def try_get_type_from_string(self, type_string):
        try:
            return self.get_type_from_string(type_string)
        except KeyError:
            return unknown

    def __repr__(self):
        return ", ".join(sorted(_Types._ia_types.keys()))

    @staticmethod
    def _get_list():
        return _Types._ia_types.values()


supported_types = _Types(_Types._get_list())
