//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

from collections import OrderedDict
from intelanalytics.core.iatypes import valid_data_types

import json
import intelanalytics.core.iatypes as iatypes


class NumpyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, iatypes.float32) or isinstance(obj, iatypes.float64):
            return float(obj)
        if isinstance(obj, iatypes.int32) or isinstance(obj, iatypes.float64):
            return int(obj)
        if isinstance(obj, iatypes.vector):
            return obj.tolist()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class Row(object):

    def __init__(self, schema, data=None):
        """
        Expects schema to as list of tuples
        """
        self.__schema_dict = OrderedDict(schema)
        self.__data = [] if data is None else data  # data is an array of strings right now

    def __getattr__(self, name):
        if name != "_Row__schema_dict" and name in self.__schema_dict.keys():
            return self[name]
        return super(Row, self).__getattribute__(name)

    def __getitem__(self, key):
        try:
            if isinstance(key, slice):
                raise TypeError("Index slicing a row is not supported")
            if isinstance(key, list):
                return [self._get_cell_value(k) for k in key]
            if isinstance(key, int):
                return self._get_cell_value_by_index(key)
            return self._get_cell_value(key)
        except KeyError:
            raise KeyError("Column name " + str(key) + " not present.")

    def __len__(self):
        return len(self.__schema_dict)

    def __iter__(self):
        return self.items().__iter__()

    def json_dumps(self):
        return json.dumps(self.__data, cls=NumpyJSONEncoder)

    def _get_data(self):
        return self.__data

    def _set_data(self, value):
        self.__data = value

    def keys(self):
        return self.__schema_dict.keys()

    def values(self):
        return [self._get_cell_value(k) for k in self.keys()]

    def types(self):
        return self.__schema_dict.values()

    def items(self):
        return zip(self.keys(), self.values())

    def get_cell_type(self, key):
        try:
            return self.__schema_dict[key]
        except KeyError:
            raise ValueError("'%s' is not in the schema" % key)

    def _get_cell_value(self, key):
        try:
            index = self.__schema_dict.keys().index(key)  # could improve speed here...
        except ValueError:
            raise KeyError(key)
        return self._get_cell_value_by_index(index)

    def _get_cell_value_by_index(self, index):
        # converts the value into the proper data type
        try:
            dtype = self.__schema_dict.values()[index]
        except IndexError:
            raise IndexError("Internal Error: improper index %d used in schema with %d columns"
                             % (index, len(self.__schema_dict)))
        try:
            return valid_data_types.cast(self.__data[index], dtype)
        except IndexError:
            raise IndexError("Internal Error: improper index %d used in row data with %d columns"
                             % (index, len(self.__data)))
