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
Meta-programming - dynamically adding commands to api objects or building auto*.py
"""

__all__ = ['CommandLoadable', 'load_loadable']


import logging
logger = logging.getLogger(__name__)

import sys
import inspect
import datetime
from collections import deque

_created_classes = {}
"""All the dynamically created loadable classes, added as they are created"""


# Constants
IA_URI = '_id'  # TODO: switch over when Anahita finishes the IA_URI switch from _id

COMMAND_DEF = '_command_def'
COMMAND_PREFIXES = '_command_prefixes'
INTERMEDIATE_NAME = '_intermediate_name'
INTERMEDIATE_CLASS = '_intermediate_class'
LOADED_COMMANDS = '_loaded_commands'
LOADED_INTERMEDIATE_CLASSES = '_loaded_intermediate_classes'
MUTED_COMMAND_NAMES = '_muted_command_names'

EXECUTE_COMMAND_FUNCTION_NAME = 'execute_command'
ALIASED_EXECUTE_COMMAND_FUNCTION_NAME = 'aliased_execute_command'


class CommandNotLoadedError(NotImplementedError):
    pass


class CommandLoadable(object):
    """
    Base class for objects which accept dynamically created members based on external info

    i.e. the class is 'loadable' with commands

    Inheritors must...

    1.  Implement the following:
        instance attribute '_ia_uri' : str or int  (See IA_URI constant)
            identifies the instance to the server

        class attribute 'command_prefixes' : list of str (See COMMAND_PREFIXES constant)
            The prefixes of those commands which will be loaded into this class
            e.g. ['graph'] to accept commands like 'graph/ml/page_rank'

        (And optionally)
        class attribute 'muted_commands_names' : list of str (See MUTED_COMMAND_NAMES constant)
            The names of the commands which should not be exposed publicly in this class
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
        if not hasattr(self._get_root_object(), IA_URI):
            raise TypeError("CommandLoadable inheritor %s instance lacks implementation for '%s'"
                            % (self.__class__, IA_URI))

        # instantiate the loaded intermediate classes
        if hasattr(self.__class__, LOADED_INTERMEDIATE_CLASSES):
            logger.debug("%s has intermediate classes to instantiate", self.__class__)
            intermediate_classes = getattr(self.__class__, LOADED_INTERMEDIATE_CLASSES)
            for intermediate in intermediate_classes:
                private_member_name = get_private_name(getattr(intermediate, INTERMEDIATE_NAME))
                if not hasattr(self, private_member_name):
                    logger.debug("Instantiating intermediate class %s as %s", intermediate, private_member_name)
                    instance = intermediate(self, *args, **kwargs)  # pass self as parent
                    setattr(self, private_member_name, instance)

    def _get_root_object(self):
        """internal method to enable intermediate member class to get the original loadable class instance"""
        walker = self
        while walker._loadable_parent is not None:
            walker = walker._loadable_parent
        return walker

    def _get_root_ia_uri(self):
        return getattr(self._get_root_object(), IA_URI)

    @classmethod
    def _should_load(cls, command):
        return command.prefix in getattr(cls, COMMAND_PREFIXES)\
               and command.name not in getattr(cls, MUTED_COMMAND_NAMES)


def get_private_name(name):
    return '_' + name


def get_intermediate_class_name(parent_class, intermediate_name):
    """Returns the name for intermediate class based on its parent class and intermediate name"""
    # Example.   get_intermediate_class_name(BigGraph, 'ml') returns 'BigGraphMl'
    prefix = parent_class.__name__ if parent_class else ''
    suffix = intermediate_name[0].upper() + intermediate_name[1:]
    return prefix + suffix


def get_intermediate_class(parent_class, intermediate_name):
    """Creates and/or gets the intermediate class"""
    class_name = get_intermediate_class_name(parent_class, intermediate_name)
    # Validate that if existing member of such a name is already there, it's a getter for the loadable class we want
    if hasattr(parent_class, intermediate_name):
        prop = getattr(parent_class, intermediate_name)
        if not all([type(prop) is property,
                    hasattr(prop.fget, INTERMEDIATE_CLASS),
                    CommandLoadable in inspect.getmro(getattr(prop.fget, INTERMEDIATE_CLASS)),
                    getattr(prop.fget, INTERMEDIATE_CLASS).__name__ == class_name]):
            raise ValueError("CommandLoadable Class %s already has a member %s which does not access a loadable class."
                             % (parent_class.__name__, intermediate_name))
    return _created_classes.get(class_name, None) or create_intermediate_class(parent_class, intermediate_name)


def create_intermediate_class(parent_class, intermediate_name):
    class_name = get_intermediate_class_name(parent_class, intermediate_name)
    doc = "Contains %s functionality for %s" % (intermediate_name, parent_class.__name__)
    intermediate_class = create_loadable_class(class_name, parent_class, doc)
    setattr(intermediate_class, INTERMEDIATE_NAME, intermediate_name)
    setattr(intermediate_class, LOADED_COMMANDS, [])
    return intermediate_class


def create_loadable_class(new_class_name, namespace_obj, doc):
    """Dynamically create a class type with the given name and namespace_obj"""
    new_class = type(str(new_class_name),
                     (CommandLoadable,),
                     {'__doc__': doc, '__module__': namespace_obj.__module__})
    # assign to its module, and to globals
    # http://stackoverflow.com/questions/13624603/python-how-to-register-dynamic-class-in-module
    setattr(sys.modules[new_class.__module__], new_class.__name__, new_class)
    globals()[new_class.__name__] = new_class
    _created_classes[new_class.__name__] = new_class
    return new_class


def add_intermediate_class(parent_class, intermediate_name):
    """Add the class to the list of classes which the loadable class should instantiate
       during its __init__.  It will instantiate it as a private member, with a leading
       underscore character.  So this method also adds a getter property to the
       loadable_class definition"""
    intermediate_class = get_intermediate_class(parent_class, intermediate_name)

    # Add a property getter which returns an instance member variable of the
    # same name prefixed w/ an underscore, per convention
    prop = create_intermediate_property(intermediate_class)
    setattr(parent_class, intermediate_name, prop)

    if not hasattr(parent_class, LOADED_INTERMEDIATE_CLASSES):
        setattr(parent_class, LOADED_INTERMEDIATE_CLASSES, set())
    getattr(parent_class, LOADED_INTERMEDIATE_CLASSES).add(intermediate_class)

    return intermediate_class


def check_loadable_class(cls):
    if not hasattr(cls, COMMAND_PREFIXES):
        raise TypeError("CommandLoadable inheritor %s lacks implementation for '%s'" % (cls, COMMAND_PREFIXES))
    if not hasattr(cls, MUTED_COMMAND_NAMES):
        setattr(cls, MUTED_COMMAND_NAMES, [])


def load_loadable(loadable_class, command_defs, execute_command_function):
    """Adds attributes dynamically to the loadable_class"""
    check_loadable_class(loadable_class)
    for command in command_defs:
        if loadable_class._should_load(command):
            function = create_function(command, execute_command_function)
            # First add any intermediate member classes to provide intended scoping
            current_class = loadable_class
            for intermediate_name in command.intermediates:
                current_class = add_intermediate_class(current_class, intermediate_name)
            add_command(current_class, command, function)


def add_command(loadable_class, command_def, function):
    # Add the function if it doesn't already exist
    if not hasattr(loadable_class, command_def.name):
        setattr(loadable_class, command_def.name, function)
        if not hasattr(loadable_class, LOADED_COMMANDS):
            setattr(loadable_class, LOADED_COMMANDS, [])
        getattr(loadable_class, LOADED_COMMANDS).append(command_def)
        logger.debug("Added function %s to class %s", command_def.name, loadable_class)


def get_execute_command_function_text():
    return '''
def {execute_command}(_name, **kwargs):
    """Validates arguments and calls execute_command"""
    from intelanalytics.rest.command import {execute_command} as {alias}
    arguments = validate_arguments(kwargs, _parameters[_name])
    return {alias}(_name, **arguments)
'''.format(execute_command=EXECUTE_COMMAND_FUNCTION_NAME, alias=ALIASED_EXECUTE_COMMAND_FUNCTION_NAME)


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
        if parameter.data_type is list:
            if v is not None and (isinstance(v, basestring) or not hasattr(v, '__iter__')):
                validated[k] = [v]
    return validated


def create_execute_command_function(command_def, execute_command_function):
    """
    Creates the appropriate execute_command for the command_def by closing
    over the parameter info for validating the arguments during usage
    """
    parameters = command_def.parameters
    def execute_command(name, **kwargs):
        arguments = validate_arguments(kwargs, parameters)
        return execute_command_function(name, **arguments)
    return execute_command


def get_self_argument_text():
    """Produces the text for argument to use for self in a command call"""
    return "self.%s().%s" % (CommandLoadable._get_root_object.__name__, IA_URI)


def get_function_text(command_def, validate_args=False):
    """Produces python code text for a command to be inserted into python modules"""
    calling_args = []
    signature_args = []
    name_of_self = ''
    for param in command_def.parameters:
        if param.use_self:
            if name_of_self:
                raise RuntimeError("Internal Metaprogramming Error: more than one parameter wants to be called 'self'")
            name = 'self'
            name_of_self = param.name
        else:
            name = param.name
        signature_args.append(name if not param.optional else "%s=%s" % (param.name, _default_val_to_str(param)))
        calling_args.append(param.name)
    text_format = '''def {func_name}({signature_args}):
    """
    {doc}
    """
    return {exec_func_name}('{command_full_name}', {kwargs_str})
'''
    text = text_format.format(func_name=command_def.name,
                              signature_args=', '.join(signature_args),
                              doc=command_def.doc,
                              exec_func_name=EXECUTE_COMMAND_FUNCTION_NAME,
                              command_full_name=command_def.full_name,
                              kwargs_str=", ".join(["%s=%s" % (a, a if a != name_of_self else get_self_argument_text()) for a in calling_args]))
    return text


def _default_val_to_str(param):
    return param.default if param.default is None or param.data_type not in [str, unicode] else "'%s'" % param.default


def create_function(command_def, execute_command_function=None):
    """Creates the function which will appropriately call execute_command for this command"""
    execute_command = create_execute_command_function(command_def, execute_command_function)
    func_text = get_function_text(command_def)
    func_code = compile(func_text, '<string>', "exec")
    func_globals = {}
    eval(func_code, {EXECUTE_COMMAND_FUNCTION_NAME: execute_command}, func_globals)
    function = func_globals[command_def.name]
    function.command = command_def
    function.__doc__ = command_def.doc
    return function


def get_property_text(intermediate_name):
    return """@property
def %s(self):
    \"""
    %s
    \"""
    return self.%s
    """ % (intermediate_name, _get_property_doc(intermediate_name), get_private_name(intermediate_name))


def create_intermediate_property(intermediate_class):
    intermediate_name = getattr(intermediate_class, INTERMEDIATE_NAME)
    private_name = get_private_name(intermediate_name)

    def fget(self):
        return getattr(self, private_name)
    setattr(fget, INTERMEDIATE_CLASS, intermediate_class)  # set the intermediate class for the getter
    doc = _get_property_doc(intermediate_name)
    return property(fget=fget, doc=doc)


def _get_property_doc(intermediate_name):
    return "Access to object's %s functionality" % intermediate_name  # vanilla doc string


#
# auto*.py generation
#

def get_auto_module_text(loaded_class):
    return "\n".join([get_file_header_text(loaded_class),
                      get_loaded_base_class_text(loaded_class),
                      get_intermediate_classes_text(loaded_class),
                      get_execute_command_function_text(),
                      get_parameters_dict_text(loaded_class)])


def get_file_header_text(loaded_class):
    return """##############################################################################
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

# Auto-generated file for %s commands (%s)
#
# **DO NOT EDIT**


from intelanalytics.core.iatypes import *
from intelanalytics.core.metaprog import CommandLoadable, validate_arguments
from intelanalytics.core.command import Parameter
""" % (loaded_class.__name__, datetime.datetime.now().isoformat())


def get_loaded_base_class_name(loaded_class):
    return CommandLoadable.__name__ + loaded_class.__name__


def get_loaded_base_class_text(loaded_class):
    """
    Produces code text for the base class from which the main loadable class
    will inherit the commands --i.e. the main class of the auto*.py file
    """
    return """
class %s(CommandLoadable):
    \"""
    Contains commands for %s provided by the server
    \"""

%s
""" % (get_loaded_base_class_name(loaded_class),
       loaded_class.__name__,
       get_members_text(loaded_class) or 'pass')


def get_intermediate_classes_text(loaded_class):
    """
    Produces code text for dynamically created loadable classes needed as intermediate objects
    """
    if not hasattr(loaded_class, LOADED_INTERMEDIATE_CLASSES):
        return ''

    names = []
    lines = []
    q = deque(getattr(loaded_class, LOADED_INTERMEDIATE_CLASSES))
    while len(q):
        c = q.pop()
        names.append(c.__name__)
        lines.append("""
class %s(CommandLoadable):
    \"""
%s
    \"""

    def __init__(self, *args, **kwargs):
        %s.__init__(self, *args, **kwargs)

%s
""" % (c.__name__, indent(c.__doc__), CommandLoadable.__name__, get_members_text(c)))
        if hasattr(c, LOADED_INTERMEDIATE_CLASSES):
            q.appendleft(getattr(c, LOADED_INTERMEDIATE_CLASSES))
    if names:
        lines.append("__all__ = [%s]" % (', '.join(["'%s'" % name for name in names])))
    return "\n".join(lines)


def get_members_text(loaded_class):
    """
    Produces code text for all the commands (both functions and properties)
    that have been loaded into the loadable class
    """
    lines = []
    init_lines = []
    if hasattr(loaded_class, LOADED_COMMANDS):
        for command in getattr(loaded_class, LOADED_COMMANDS):
            lines.append(indent(get_function_text(command)))
    if hasattr(loaded_class, LOADED_INTERMEDIATE_CLASSES):
        for intermediate_class in getattr(loaded_class, LOADED_INTERMEDIATE_CLASSES):
            member_name = getattr(intermediate_class, INTERMEDIATE_NAME)
            init_lines.append(indent("self.%s = %s(self)" % (get_private_name(member_name), intermediate_class.__name__)))
            lines.append(indent(get_property_text(member_name)))
    if init_lines:
        lines.insert(0, indent("""def __init__(self, *args, **kwargs):
    %s.__init__(self)
%s
""" % (CommandLoadable.__name__, "\n".join(init_lines))))

    return "\n".join(lines)


def get_parameters_dict_text(loaded_class):
    """
    Produces code text to define a dict which contains all the Parameter info
    for the commands defined in this auto*.py file.
    """
    # The parameter info must be available for argument coercion when the
    # commands are executed.  We must write it in hard-coded text since we
    # are not relying on the server info once the auto*.py file is written.
    q = deque([loaded_class])
    lines = []
    while len(q):
        current_class = q.pop()
        if hasattr(current_class, LOADED_COMMANDS):
            lines.extend([_get_parameters_text(loaded_class, command)  # loaded_class needs to stay root obj
                for command in getattr(current_class, LOADED_COMMANDS)])
        if hasattr(current_class, LOADED_INTERMEDIATE_CLASSES):
            q.extendleft(getattr(current_class, LOADED_INTERMEDIATE_CLASSES))

    return """
# Parameter definitions for argument validation.  Disregard values for default and doc.  They are overridden to None.
_parameters = {
    %s
}""" % ",\n    ".join(lines)


def _get_parameters_text(loaded_class, command_def):
    return "'%s':[\n        %s]" % (command_def.full_name, ",\n        ".join(_get_parameter_construction_text(loaded_class, p) for p in command_def.parameters))


def _get_parameter_data_type_text(loaded_class, parameter):
    # TODO - remove the None compensation --this should never happen (binColumn and maybe others are producing a None currently)
    return 'None' if parameter.data_type is None\
        else get_loaded_base_class_name(loaded_class) if parameter.data_type is loaded_class\
        else parameter.data_type.__name__


def _get_parameter_construction_text(loaded_class, parameter):
    return "Parameter(name='%s', data_type=%s, use_self=%s, optional=%s, default=None, doc=None)" % (parameter.name, _get_parameter_data_type_text(loaded_class, parameter), parameter.use_self, parameter.optional)


def indent(text, spaces=4):
    indentation = ' ' * spaces
    return "\n".join([indentation + line for line in text.split('\n')])
