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

from intelanalytics.meta.clientside import get_api_decorator, arg, returns
from intelanalytics.meta.names import entity_type_to_collection_name, upper_first
from intelanalytics.meta.metaprog import get_entity_class_from_store, set_entity_collection


def name_support(term):
    _term = term
    def _add_name_support(cls):
        add_named_object_support(cls, _term)
        return cls
    return _add_name_support


def add_named_object_support(obj_class, obj_term):
    from intelanalytics.rest.iaserver import server
    from intelanalytics.rest.command import execute_command
    _NamedObjectsFunctionFactory(obj_class, obj_term, server, execute_command)
    # the act of creation is sufficient, they'll get registered via decorations during install


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
        get_name.__name__ = '__name'
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
        get_name.__doc__ = doc

        def set_name(self, value):
            arguments = {obj_term: self._id, "new_name": value}
            execute_command(obj_term + "/rename", self, **arguments)
        set_name.__name__ = '__name'
        from intelanalytics.meta.context import get_api_context_decorator
        api_set_name = get_api_context_decorator(module_logger)(set_name)

        name_prop = property(fget=get_name, fset=api_set_name, fdel=None, doc=doc)
        return get_api_decorator(module_logger, parent_class_name=obj_class.__name__)(name_prop)

    def create_get_object_names(self):
        get_object_names_name = "__get_%s_names" % self._term
        rest_collection = self._term + 's'
        module_logger = self._module_logger
        http = self._http

        def get_object_names():
            module_logger.info(get_object_names_name)
            r = http.get(rest_collection)
            payload = r.json()
            return [item.get('name', None) for item in payload]
        set_entity_collection(get_object_names, entity_type_to_collection_name(self._term))  # so meta knows where it goes
        get_object_names.__name__ = get_object_names_name
        get_object_names.__doc__ = """Retrieve names for all the {obj_term} objects on the server.""".format(obj_term=self._term)
        # decorate the method with api and arg, which will also give a nice command def for setting the doc_stub
        api_decorator = get_api_decorator(module_logger, parent_class_name="_BaseGlobals")
        returns_decorator = returns(list, "List of names")
        decorated_method = api_decorator(returns_decorator(get_object_names))
        return decorated_method

    def create_get_object(self):
        get_object_name = "__get_%s" % self._term
        rest_target = '%ss' % self._term
        rest_target_with_name = '%s?name=' % rest_target
        module_logger = self._module_logger
        http = self._http
        term = self._term
        obj_class = self._class
        get_class = get_entity_class_from_store

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
        set_entity_collection(get_object, entity_type_to_collection_name(self._term))  # so meta knows where it goes
        get_object.__name__ = get_object_name
        get_object.__doc__ = """Get handle to a {obj_term} object.""".format(obj_term=self._term)
        # decorate the method with api and arg, which will also give a nice command def for setting the doc_stub
        api_decorator = get_api_decorator(module_logger, parent_class_name="_BaseGlobals")
        arg_decorator = arg(name="identifier",
                            data_type="str | int",
                            description="Name of the %s to get" % self._term)
        # Determining the return type is difficult, because we don't know in advance what specific type of entity
        # we will get (like Frame or VertexFrame or ?).  We choose to go with the general entity type, like "Frame",
        # because it is likely the most common case and also has backing for SPA (i.e. _BaseFrame does not get defined
        # in the DocStubs)  Btw, we know "Model" will fail SPA --there is no "general" Model class.
        returns_type = upper_first(self._term)  # so, "frame" -> "Frame"
        returns_decorator = returns(returns_type, "%s object" % self._term)
        decorated_method = api_decorator(returns_decorator(arg_decorator(get_object)))
        return decorated_method

    def create_drop_objects(self):
        # create local vars for better closures:
        drop_objects_name = "__drop_%ss" % self._term
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
        set_entity_collection(drop_objects, entity_type_to_collection_name(self._term))  # so meta knows where it goes
        drop_objects.__name__ = drop_objects_name
        drop_objects.__doc__ = """Deletes the {obj_term} on the server.""".format(obj_term=obj_term)
        # decorate the method with api and arg, which will also give a nice command def for setting the doc_stub
        api_decorator = get_api_decorator(module_logger, parent_class_name="_BaseGlobals")
        arg_decorator = arg(name="items",
                            data_type="[ str | {obj_term} object | list [ str | {obj_term} objects ]]".format(obj_term=obj_term),
                            description="Either the name of the {obj_term} object to delete or the {obj_term} object itself".format(obj_term=obj_term))
        decorated_method = api_decorator(arg_decorator(drop_objects))
        return decorated_method
