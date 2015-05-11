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
intel_analytics definitions for Data Types
"""

# TODO - consider server providing types, similar to commands

__all__ = ['valid_data_types', 'ignore', 'unknown', 'float32', 'float64', 'int32', 'int64', 'vector']

import numpy as np
import json
import re

# alias numpy types
float32 = np.float32
float64 = np.float64
int32 = np.int32
int64 = np.int64


class _Vector(object):

    base_type = np.ndarray
    re_pattern = re.compile(r"^vector\((\d+)\)$")

    def __init__(self, length):
        self.length = int(length)
        self.is_complex_type = True
        self.constructor = self._get_constructor()

    def _get_constructor(self):
        length = self.length

        def constructor(value):
            """
            Creates a numpy array from a value, which can be one of many types
            """
            if value is None:
                return None
            try:
                # first try numpy's constructor
                array = np.array(value, dtype=np.float64)  # ensures the array is entirely made of doubles
            except:
                # also support json or comma-sep string
                if valid_data_types.value_is_string(value):
                    try:
                        value = json.loads(value)
                    except:
                        value = [np.float64(item.strip()) for item in value.split(',') if item]
                    array = np.array(value, dtype=np.float64)  # ensures the array is entirely made of doubles
                else:
                    raise

            array = np.atleast_1d(array)  # numpy thing, so that vectors of size 1 will still have dimension and length
            if len(array) != length:
                raise ValueError("Could not construct vector in Python Client.  Expected vector of length %s, but received length %d" % (length, len(array)))
            return array
        return constructor

    @staticmethod
    def get_from_string(data_type_str):
        return _Vector(_Vector.re_pattern.match(data_type_str).group(1))

    def __repr__(self):
        return "vector(%d)" % self.length


vector = _Vector


class _Ignore(object):
    """Ignore type used for schemas during file import"""
    pass

ignore = _Ignore


class _Unknown(object):
    """Unknown type used when type is indeterminate"""
    pass

unknown = _Unknown


# map types to their string identifier
_primitive_type_to_str_table = {
    #bool: "bool", TODO
    #bytearray: "bytearray", TODO
    #dict: "dict", TODO
    float32: "float32",
    float64: "float64",
    int32: "int32",
    int64: "int64",
    #list: "list", TODO
    unicode: "unicode",
    ignore: "ignore"
}

# build reverse map string -> type
_primitive_str_to_type_table = dict([(s, t) for t, s in _primitive_type_to_str_table.iteritems()])

_primitive_alias_type_to_type_table = {
    float: float64,
    int: int32,
    long: int64,
    str: unicode,
    #list: vector,
}

_primitive_alias_str_to_type_table = dict([(alias.__name__, t) for alias, t in _primitive_alias_type_to_type_table.iteritems()])


def get_float_constructor(float_type):
    """Creates special constructor for floating point types which handles nan, inf, -inf"""
    ft = float_type

    def float_constructor(value):
        result = ft(value)
        if np.isnan(result) or result == np.inf or result == -np.inf:  # this is 5x faster than calling np.isfinite()
            return None
        return ft(value)
    return float_constructor


class _DataTypes(object):
    """
    Provides functions with define and operate on supported data types.
    """

    def __contains__(self, item):
        try:
            self.validate(item)
            return True
        except ValueError:
            return False

    def __repr__(self):
        aliases = "\n(and aliases: %s)" % (", ".join(sorted(["%s->%s" % (alias.__name__, self.to_string(data_type)) for alias, data_type in _primitive_alias_type_to_type_table.iteritems()])))
        return ", ".join(sorted(_primitive_str_to_type_table.keys() + ["vector(n)"])) + aliases

    @staticmethod
    def value_is_number(value):
        """get bool indication that value is a number, like int, long, float"""
        return isinstance(value, (int, long, float, int32, int64, float32, float64))   # not supporting complex

    @staticmethod
    def value_is_string(value):
        """get bool indication that value is a string, whether str or unicode"""
        return isinstance(value, basestring)

    @staticmethod
    def value_is_missing_value(value):
        return value is None or (type(value) in [float32, float64, float] and (np.isnan(value) or value in [np.inf, -np.inf]))

    @staticmethod
    def get_primitive_data_types():
        return _primitive_type_to_str_table.keys()

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
        valid_data_type = _DataTypes.get_from_type(data_type)
        try:
            return _primitive_type_to_str_table[valid_data_type]
        except KeyError:
            # complex data types should use their repr
            return repr(valid_data_type)

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
            return _primitive_str_to_type_table[data_type_str]
        except KeyError:
            try:
                return _primitive_alias_str_to_type_table[data_type_str]
            except KeyError:
                try:
                   return vector.get_from_string(data_type_str)
                except:
                   raise ValueError("Unsupported type string '%s' " % data_type_str)


    @staticmethod
    def is_primitive_type(data_type):
        return data_type in _primitive_type_to_str_table or data_type in _primitive_alias_type_to_type_table

    @staticmethod
    def is_complex_type(data_type):
        try:
            return data_type.is_complex_type
        except AttributeError:
            return False

    @staticmethod
    def is_primitive_alias_type(data_type):
        return data_type in _primitive_alias_type_to_type_table

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
        if _DataTypes.is_primitive_alias_type(data_type):
            return _primitive_alias_type_to_type_table[data_type]
        if _DataTypes.is_primitive_type(data_type) or _DataTypes.is_complex_type(data_type):
            return data_type
        raise ValueError("Unsupported type %s" % data_type)

    @staticmethod
    def validate(data_type):
        """Raises a ValueError if data_type is not a valid data_type"""
        _DataTypes.get_from_type(data_type)

    @staticmethod
    def get_constructor(to_type):
        """gets the constructor for the to_type"""
        try:
            return to_type.constructor
        except AttributeError:
            if to_type == float64 or to_type == float32:
                return get_float_constructor(to_type)

            def constructor(value):
                if value is None:
                    return None
                return to_type(value)
            return constructor

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
        >>> valid_data_types.cast(np.inf, float32)
        None
        """
        if _DataTypes.value_is_missing_value(value):  # Special handling for missing values
            return None
        elif _DataTypes.is_primitive_type(to_type) and type(value) is to_type:  # Optimization
            return value
        try:
            constructor = _DataTypes.get_constructor(to_type)
            result = constructor(value)
            return None if _DataTypes.value_is_missing_value(result) else result
        except Exception as e:
            raise ValueError(("Unable to cast to type %s\n" % to_type) + str(e))


valid_data_types = _DataTypes()

def numpy_to_bson_friendly(obj):
    """take an object and convert it to a type that can be serialized to bson if neccessary."""
    if isinstance(obj, float32) or isinstance(obj, float64):
        return float(obj)
    if isinstance(obj, int32):
        return int(obj)
    if isinstance(obj, vector.base_type):
        return obj.tolist()
    # Let the base class default method raise the TypeError
    return obj