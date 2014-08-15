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
Meta-programming - dynamically adding commands to api objects
"""

__all__ = ['CommandLoadable', 'load_loadable']

import logging
logger = logging.getLogger(__name__)

import sys
import inspect
from collections import namedtuple


_loadable_classes = {}
"""All the dynamically created loadable classes, added as they are created"""


_member_classes_to_instantiate = '_member_classes_to_instantiate'  # can be anything, just needs to be shared

_execute_command_function_name = "execute_command"  # can be anything, just needs to be shared


class CommandNotLoadedError(NotImplementedError):
    pass


class CommandLoadable(object):
    """
    Base class for objects which accept dynamically created new members based on external info

    i.e. the class is 'loadable' with commands

    Inheritors must...

    1.  Implement the following:
        class attribute 'command_prefixes' : list of str
            The prefixes of those commands which will be loaded into this class
            e.g. ['graph'] to accept commands like 'graph/ml/page_rank'

        instance attribute '_id' : str or int
            _id which identifies the instance to the server

        (And optionally)
        class field 'command_mute_list' : list of str
            commands which should not be exposed publicly in this class
            e.g. ['load'] to mute 'load' publicly, i.e. it's only used internally

    2.  Call CommandLoadable.__init__(self) in its own __init__, AFTER the class
        has been loaded.  For example, in the __init__ function, make a call to
        load_loadable before calling the CommandLoadable.__init__ method
    """


    def __init__(self, parent=None, *args, **kwargs):
        logger.debug("Enter CommandLoadable.__init__ from class %s" % self.__class__)
        # by convention, the parent Type is passed as the first arg
        # capture parent to enable walking ancestry
        self._loadable_parent = parent
        try:
            self._get_id()
        except AttributeError:
            raise TypeError("CommandLoadable inheritor %s instance lacks implementation for '_id'" % self.__class__)

        # _member_classes_to_instantiate is dynamically added
        if hasattr(self.__class__, _member_classes_to_instantiate):
            logger.debug("Has %s", _member_classes_to_instantiate)
            member_classes = getattr(self.__class__, _member_classes_to_instantiate)
            for name, cls in member_classes:
                logger.debug("Instantiating dynamic member class %s", name)
                instance = cls(self, *args, **kwargs)  # pass self as parent
                setattr(self, '_' + name, instance)  # '_' to make private for class getter already dynamically added

    def _get_id(self):
        """internal method to enable intermediate member class to get the id of the original loadable class"""
        # TODO - augment when/if the _id/uri convention gets ironed out
        walker = self
        while walker._loadable_parent is not None:
            walker = walker._loadable_parent
        return walker._id


MemberClass = namedtuple('MemberClass', ['member_name', 'cls'])


def get_member_class_name(parent_class, member_name):
    """Returns the name for member class based on its parent class and member name"""
    prefix = parent_class.__name__ if parent_class else ''
    suffix = member_name[0].upper() + member_name[1:]
    return prefix + suffix


def get_member_class(parent_class, member_name):
    """Creates and/or gets the member class"""
    class_name = get_member_class_name(parent_class, member_name)
    # Validate that if existing member of such a name is already there, it's a getter for the loadable class we want
    if hasattr(parent_class, member_name):
        prop = getattr(parent_class, member_name)
        if not all([type(prop) is property,
                    hasattr(prop.fget, 'loadable_class'),
                    CommandLoadable in inspect.getmro(prop.fget.loadable_class),
                    prop.fget.loadable_class.__name__ == class_name]):
            raise ValueError("CommandLoadable Class %s already has a member %s.  Will not override dynamically."
                             % (parent_class.__name__, member_name))
    loadable_class = _loadable_classes.get(class_name, None) or create_loadable_class(class_name, parent_class)
    return loadable_class


def add_member_class(parent_class, member_name):
    """Add the class to the list of classes which the loadable class should instantiate
       during its __init__.  It will instantiate it as a private member, with a leading
       underscore character.  So this method also adds a getter property to the
       loadable_class definition"""
    member_class = get_member_class(parent_class, member_name)

    if not hasattr(parent_class, _member_classes_to_instantiate):
        setattr(parent_class, _member_classes_to_instantiate, set())
    getattr(parent_class, _member_classes_to_instantiate).add(MemberClass(member_name, member_class))

    # Add a property getter which returns an instance member variable of the
    # same name prefixed w/ an underscore, per convention
    private_name = '_' + member_name

    def fget(self):
        return getattr(self, private_name)
    fget.loadable_class = member_class
    doc = "Access to object's %s functionality" % member_name  # vanilla doc string
    setattr(parent_class, member_name, property(fget=fget, doc=doc))
    return member_class


def create_loadable_class(new_class_name, namespace_obj, doc=None):
    """Dynamically create a class type with the given name and namespace_obj"""
    if not doc:
        doc = "Auto-generated class to scope functionality"
    new_class = type(str(new_class_name),
                     (CommandLoadable,),
                     {'__doc__': doc, '__module__': namespace_obj.__module__})
    # assign to its module, and to globals
    # http://stackoverflow.com/questions/13624603/python-how-to-register-dynamic-class-in-module
    setattr(sys.modules[new_class.__module__], new_class.__name__, new_class)
    globals()[new_class.__name__] = new_class
    _loadable_classes[new_class.__name__] = new_class
    return new_class


def _default_val_to_str(param):
    return param.default if param.default is None or param.data_type not in [str, unicode] else "'%s'" % param.default


def make_code_text(command_def):
    """Writes python code text for this command to be inserted into python modules"""
    calling_args = []
    signature_args = []
    for param in command_def.parameters:
        calling_args.append(param.name)
        signature_args.append(param.name if not param.optional else "%s=%s" % (param.name, _default_val_to_str(param)))
    text = 'def %s(%s):\n    """%s"""\n    return %s(\'%s\', %s)\n' % (command_def.name,
                                                                       ", ".join(signature_args),
                                                                       command_def.doc,
                                                                       _execute_command_function_name,
                                                                       command_def.full_name,
                                                                       ", ".join(["%s=%s" % (a, a) for a in calling_args]))
    logger.debug("Created code text:\n%s", text)
    return text


def make_function(command_def, execute_command_function=None):
    """Creates the function which will appropriately call execute_command for this command"""
    execute_command = make_execute_command_function(command_def, execute_command_function)
    func_text = make_code_text(command_def)
    func_code = compile(func_text, '<string>', "exec")
    func_globals = {}
    eval(func_code, {_execute_command_function_name: execute_command}, func_globals)
    function = func_globals[command_def.name]
    function.command = command_def.json_schema
    function.__doc__ = command_def.doc
    return function


def validate_arguments(arguments, parameters):
    """
    Returns validated and possibly re-cast arguments

    Use parameter definitions to make sure the arguments conform.  This function
    is closure over in the dynamically generated execute command function
    """
    validated = {}
    for (k, v) in arguments.items():
        try:
            parameter = [p for p in parameters if p.name == k][0]
        except IndexError:
            raise ValueError("No parameter named '%s'" % k)
        validated[k] = v
        if parameter.use_self:
            validated[k] = v._get_id()
        if parameter.data_type is list:
            if isinstance(v, basestring) or not hasattr(v, '__iter__'):
                validated[k] = [v]
    return validated


def make_execute_command_function(command_def, execute_command_function):
    parameters = command_def.parameters
    def execute_command(name, **kwargs):
        arguments = validate_arguments(kwargs, parameters)
        return execute_command_function(name, arguments)
    return execute_command


def check_loadable_class(cls):
    if not hasattr(cls, "command_prefixes"):
        raise TypeError("CommandLoadable inheritor %s lacks implementation for 'command_prefixes'" % cls)
    if not hasattr(cls, "command_mute_list"):
            setattr(cls, "command_mute_list", [])


def load_loadable(loadable_class, command_defs, execute_command_function):  # func_descriptors, as_staticmethods=False):
    """Adds attributes dynamically to the loadable_class"""
    check_loadable_class(loadable_class)
    for command in command_defs:
        if command.prefix not in loadable_class.command_prefixes or command.name in loadable_class.command_mute_list:
            continue
        function = make_function(command, execute_command_function)
        # First add any intermediate member classes to provide intended scoping
        current_class = loadable_class
        for intermediate_name in command.intermediates:
            current_class = add_member_class(current_class, intermediate_name)
        # Then add the function if it doesn't already exist
        if not hasattr(current_class, command.name):
            setattr(current_class, command.name, function)
            logger.debug("Added function        %s to class %s", command.name, current_class)
