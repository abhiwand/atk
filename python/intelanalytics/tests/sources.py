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

class SimpleDataSource(object):

    annotation = "simple"

    def __init__(self, schema=None, rows=None, columns=None):
        if not ((rows is None) ^ (columns is None)):
            raise ValueError("Either rows or columns must be supplied")
        if schema and not isinstance(schema, OrderedDict):
            self.schema = OrderedDict(schema)
        else:
            self.schema = schema
        self.rows = rows
        self.columns = columns
        if columns:
            names = self.schema.keys()
            if len(names) != len(self.columns):
                raise ValueError("number of columns in schema not equals number of columns provided")
            for key in self.columns.keys():
                if key not in names:
                    raise ValueError("names in schema do not all match the names in the columns provided")

"""
    def to_pandas_dataframe(self):
        import numpy as np
        from pandas import DataFrame
        if self.rows:
            a = np.array(self.rows, dtype=_schema_as_numpy_dtype(self.schema))
            df = DataFrame(a)
        else:  # columns
            df = DataFrame(self.columns)
        return df
"""
def _schema_as_numpy_dtype(schema):
    return [(c, _get_numpy_dtype_from_core_type(t)) for c, t in schema.items()]

def _get_numpy_dtype_from_core_type(t):
    return object
    # if t in [str, unicode, dict, bytearray, list]:
    #     return object
    # return t
