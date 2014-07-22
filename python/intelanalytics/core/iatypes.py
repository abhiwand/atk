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

# TODO - consider server providing types, similar to commands

__all__ = ['valid_data_types', 'ignore', 'unknown', 'float32', 'float64', 'int32', 'int64', 'supported_types']

import numpy as np

# alias numpy types
float32 = np.float32
float64 = np.float64
int32 = np.int32
int64 = np.int64


class _Ignore(object):
    """Ignore type used for schemas during file import"""
    pass

ignore = _Ignore


class _Unknown(object):
    """Unknown type used when type is indeterminate"""
    pass

unknown = _Unknown


_types = {
    #bool: "bool", TODO
    #bytearray: "bytearray", TODO
    #dict: "dict", TODO
    float32: "float32",
    float64: "float64",
    int32: "int32",
    int64: "int64",
    #list: "list", TODO
    str: "str",
    unicode: "unicode",
}

_strings = dict([(s, t) for t, s in _types.iteritems()])

_alias_types = {
    float: float64,
    int: int32,
    long: int64,
}


class _DataTypes(frozenset):
    """
    Acts as frozenset of valid data types along with some conversion functions
    """

    @staticmethod
    def to_string(data_type):
        """
        Returns the string representation of the given type

        Parameters
        ----------
        data_type : type
            valid data type; if invalid, a ValueError is raised

        Returns
        -------
        result : str
            string representation

        Examples
        --------
        >>> valid_data_types.to_string(float32)
        'float32'
        """
        try:
            return _types[_DataTypes.get_from_type(data_type)]
        except ValueError:
            raise ValueError("Unsupported type %s" % data_type)

    @staticmethod
    def get_from_string(data_type_str):
        """
        Returns the data type for the given type string representation

        Parameters
        ----------
        data_type_str : str
            valid data type str; if invalid, a ValueError is raised

        Returns
        -------
        result : type
            type represented by the string

        Examples
        --------
        >>> valid_data_types.get_from_string('unicode')
        unicode
        """
        try:
            return _strings[data_type_str]
        except KeyError:
            raise ValueError("Unsupported type string '%s' " % data_type_str)

    @staticmethod
    def get_from_type(data_type):
        """
        Returns the data type for the given type (often it will return the same type)

        Parameters
        ----------
        data_type : type
            valid data type or type that may be aliased for a valid data type;
            if invalid, a ValueError is raised

        Returns
        -------
        result : type
            valid data type for given type

        Examples
        --------
        >>> valid_data_types.get_from_type(int)
        numpy.int32
        """
        if data_type in _types:
            return data_type
        try:
            return _alias_types[data_type]
        except KeyError:
            raise ValueError("Unsupported type %s" % data_type)

    @staticmethod
    def validate(data_type):
        """Raises a ValueError if data_type is not a valid data_type"""
        _DataTypes.get_from_type(data_type)

    @staticmethod
    def cast(value, to_type):
        """
        Returns the given value cast to the given type.  None is always returned as None

        Parameters
        ----------
        value : object
            value to convert by casting

        to_type : type
            valid data type to use for the cast

        Returns
        -------
        results : object
            the value cast to the to_type

        Examples
        --------
        >>> valid_data_types.cast(3, float64)
        3.0
        >>> valid_data_types.cast(4.5, str)
        '4.5'
        >>> valid_data_types.cast(None, str)
        None
        """
        if value is None or type(value) is to_type:
            return value
        try:
            return to_type(value)
        except Exception as e:
            raise ValueError(("Unable to cast to type %s\n" % to_type) + str(e))

    def __repr__(self):
        return ", ".join(sorted(_strings.keys()))


valid_data_types = _DataTypes(_types.keys())
# Awkward passing of the _types.keys().  Encapsulating these values
# inside _DataTypes requires overriding  __new__ because frozenset
# is immutable.  Doing so broke execution in Spark.  An alternative
# was to provide them in the constructor call here.  TODO - improve

supported_types = valid_data_types  # creating alias for now, until to we TODO - deprecate supported_types
