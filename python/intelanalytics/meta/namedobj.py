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
Named objects - object that have 'names' and are stored server side
"""
import sys
import logging

from intelanalytics.meta.api import get_api_decorator, api_globals
from intelanalytics.meta.metaprog import ENTITY_TYPE, set_function_doc_stub_text, get_loadable_class_from_entity_type


def name_support(term):
    _term = term
    def _add_name_support(cls):
        add_named_object_support(cls, _term)
        return cls
    return _add_name_support


def add_named_object_support(obj_class, obj_term):
    from intelanalytics.rest.connection import http
    from intelanalytics.rest.command import execute_command
    module = get_module(obj_class)
    factory = _NamedObjectsFunctionFactory(obj_class, obj_term, http, execute_command)
    for function in factory.global_functions:
        setattr(module, function.__name__, function)
        api_globals.add(function)
    setattr(obj_class, 'name', factory.class_name_property)


def get_module(cls):
    return sys.modules[cls.__module__]


def get_module_logger(module):
    return logging.getLogger(module.__name__)


class _NamedObjectsFunctionFactory(object):
    def __init__(self, obj_class, obj_term, http, execute_command):
        self._class = obj_class
        self._term = obj_term
        self._http = http
        self._execute_command = execute_command
        self._module_logger = get_module_logger(get_module(self._class))
        # globals
        self.get_object = self.create_get_object()
        self.get_object_names = self.create_get_object_names()
        self.drop_objects = self.create_drop_objects()
        # locals
        self.name_property = self.create_name_property()

    @property
    def global_functions(self):
        return (self.get_object,
                self.get_object_names,
                self.drop_objects)

    @property
    def class_name_property(self):
        return (self.name_property)

    def get_entity_id_from_name(self, entity_name):
        rest_target = '%ss' % self._term
        rest_target_with_name = '%s?name=' % rest_target
        return self._http.get(rest_target_with_name + entity_name).json()['id']

    def create_name_property(self):
        rest_target = '%ss/' % self._term
        obj_term = self._term
        obj_class = self._class
        http = self._http
        execute_command = self._execute_command
        module_logger = self._module_logger

        def get_name(self):
            r = http.get(rest_target + str(self._id))  # TODO: update w/ URI jazz
            payload = r.json()
            return payload.get('name', None)
        get_name.__name__ = 'name'
        api_get_name = get_api_decorator(module_logger)(get_name)

        def set_name(self, value):
            arguments = {obj_term: self._id, "new_name": value}
            execute_command(getattr(obj_class, ENTITY_TYPE) + "/rename", self, **arguments)
        set_name.__name__ = 'name'
        api_set_name = get_api_decorator(module_logger)(set_name)

        doc = """
        Set or get the name of the {term} object.

        Change or retrieve {term} object identification.
        Identification names must start with a letter and are limited to
        alphanumeric characters and the ``_`` character.

        Examples
        --------

        .. code::

            >>> my_{term}.name

            "csv_data"

            >>> my_{term}.name = "cleaned_data"
            >>> my_{term}.name

            "cleaned_data"
        """.format(term=self._term)
        return property(fget=api_get_name, fset=api_set_name, fdel=None, doc=doc)

    def create_get_object_names(self):
        get_object_names_name = "get_%s_names" % self._term
        rest_collection = self._term + 's'
        module_logger = self._module_logger
        http = self._http

        def get_object_names():
            module_logger.info(get_object_names_name)
            r = http.get(rest_collection)
            payload = r.json()
            return [item.get('name', None) for item in payload]
        get_object_names.__name__ = get_object_names_name
        get_object_names.__doc__ = """
        Retrieve all {obj_term} names.

        Gets the names of {obj_term} objects available for retrieval.

        Returns
        -------
        list : list of str
            Names of the all {obj_term} objects.
        """.format(obj_term=self._term)
        set_function_doc_stub_text(get_object_names, '')
        return get_api_decorator(module_logger)(get_object_names)

    def create_get_object(self):
        get_object_name = "get_%s" % self._term
        rest_target = '%ss' % self._term
        rest_target_with_name = '%s?name=' % rest_target
        module_logger = self._module_logger
        http = self._http
        term = self._term
        obj_class = self._class
        get_class = get_loadable_class_from_entity_type

        def get_object(identifier):
            module_logger.info("%s(%s)", get_object_name, identifier)
            if isinstance(identifier, basestring):
                r = http.get(rest_target_with_name + identifier)
            else:
                r = http.get('%s/%s' % (rest_target, identifier))
            try:
                entity_type = r.json()['entity_type']
            except KeyError:
                return obj_class(_info=r.json())
            else:
                if not entity_type.startswith(term):
                    raise ValueError("Object '%s' is not a %s type" % (identifier, term))
                cls = get_class(entity_type)
                return cls(_info=r.json())
        get_object.__name__ = get_object_name
        get_object.__doc__ = """
        Get access to {obj_term} object.

        Parameters
        ----------
        name : str
            String containing the name of the object.

        Returns
        -------
        class | {obj_term} object.
        """.format(obj_term=self._term)
        set_function_doc_stub_text(get_object, 'name')
        return get_api_decorator(module_logger)(get_object)

    def create_drop_objects(self):
        # create local vars for better closures:
        drop_objects_name = "drop_%ss" % self._term
        rest_target = '%ss/' % self._term
        module_logger = self._module_logger
        obj_class = self._class
        obj_term = self._term
        http = self._http

        def drop_objects(items):
            if not isinstance(items, list) and not isinstance(items, tuple):
                items = [items]
            victim_ids = {}
            for item in items:
                if isinstance(item, basestring):
                    victim_ids[item] = self.get_entity_id_from_name(item)
                elif isinstance(item, obj_class):
                    victim_ids[item.name] = item._id
                else:
                    raise TypeError("Excepted argument of type {term} or else the {term}'s name".format(term=obj_term))
            for name, id in victim_ids.items():
                module_logger.info("Drop %s %s", obj_term, name)
                http.delete(rest_target + str(id))  # TODO: update w/ URI jazz
        drop_objects.__name__ = drop_objects_name
        drop_objects.__doc__ = """
        Deletes the {obj_term} and it's data.

        Parameters
        ----------
        items : [ str | {obj_term} object | list [ str | {obj_term} objects ]]
            Either the name of the {obj_term} object to delete or the {obj_term}
            object itself
        """.format(obj_term=obj_term)
        set_function_doc_stub_text(drop_objects, 'items')
        return get_api_decorator(module_logger)(drop_objects)
