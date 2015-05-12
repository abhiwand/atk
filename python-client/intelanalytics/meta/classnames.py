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

def is_name_private(name):
    return name.startswith('_') and name != "__init__"


def name_to_private(name):
    """makes private version of the name"""
    return name if name.startswith('_') else '_' + name


def upper_first(s):
    """Makes first character uppercase"""
    return '' if not s else s[0].upper() + s[1:]


def lower_first(s):
    """Makes first character lowercase"""
    return '' if not s else s[0].lower() + s[1:]


def underscores_to_pascal(s):
    return '' if not s else ''.join([upper_first(s) for s in s.split('_')])


def pascal_to_underscores(s):
    return ''.join(["_%s" % c.lower() if c.isupper() else c for c in s])[1:]


def class_name_to_entity_type(class_name):
    if not class_name:
        raise ValueError("Invalid empty class_name, expected non-empty string")
    if class_name.startswith("_Base"):
        return pascal_to_underscores(class_name[5:])
    pieces = pascal_to_underscores(class_name).split('_')
    return "%s:%s" % (pieces[-1], '_'.join(pieces[:-1]))


def entity_type_to_class_name(entity_type):
    if not entity_type:
        raise ValueError("Invalid empty entity_type, expected non-empty string")
    parts = entity_type.split(':')
    term = underscores_to_pascal(parts[0])
    if len(parts) == 1:
        return "_Base" + term
    else:
        return underscores_to_pascal(parts[1]) + term


def entity_type_to_baseclass_name(entity_type):
    parts = entity_type.split(':')
    term = underscores_to_pascal(parts[0])
    if len(parts) == 1:
        from intelanalytics.meta.metaprog2 import CommandInstallable  # todo: refactor, remove circ dep
        return CommandInstallable.__name__
    return "_Base" + term


def entity_type_to_entity_subtype(entity_type):
    split_index = entity_type.find(':')
    return '' if split_index < 1 else entity_type[split_index+1:]


def entity_type_to_entity_basetype(entity_type):
    split_index = entity_type.find(':')
    return entity_type if split_index < 1 else entity_type[:split_index]


def entity_type_to_collection_name(entity_type):
    return entity_type_to_entity_basetype(entity_type) + "s"

