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
Meta-programming - dynamically adding commands to api objects or building doc stub *.py
"""

__all__ = ['CommandLoadable', 'load_loadable']


import logging
logger = logging.getLogger(__name__)

import sys
import datetime
import inspect
from collections import deque
from decorator import decorator

from intelanalytics.core.api import get_api_decorator
from intelanalytics.core.mute import muted_commands

_created_classes = {}
"""All the dynamically created loadable classes, added as they are created"""


# Constants
IA_URI = '_id'

COMMAND_DEF = '_command_def'
COMMAND_PREFIX = '_command_prefix'
INTERMEDIATE_NAME = '_intermediate_name'
LOADED_COMMANDS = '_loaded_commands'
LOADED_INTERMEDIATE_CLASSES = '_loaded_intermediate_classes'

EXECUTE_COMMAND_FUNCTION_NAME = 'execute_command'
ALIASED_EXECUTE_COMMAND_FUNCTION_NAME = 'aliased_execute_command'

DOC_STUB = 'doc_stub'
DOC_STUB_LOADABLE_CLASS_PREFIX = 'DocStubs'
DOC_STUB_TEXT = '_doc_stub_text'  # attribute for a function to hold on to its own doc stub text


class CommandNotLoadedError(NotImplementedError):
    pass


class CommandLoadable(object):
    """
    Base class for objects which accept dynamically created members based on external info

    i.e. the class is 'loadable' with commands

    Inheritors must...

    1.  Implement the following:
        instance attribute '_id' : str or int  (See IA_URI constant)
            identifies the instance to the server

        class attribute '_command_prefix' : list of str (See COMMAND_PREFIX constant)
            This is the prefix which identifies the class for commands loaded from the server
            e.g. 'frame:' to accept commands like 'frame:/assign_sample'

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
        if not hasattr(self, COMMAND_PREFIX):
            raise TypeError("CommandLoadable inheritor %s instance lacks implementation for '%s'"
                            % (self.__class__, COMMAND_PREFIX))

        # instantiate the loaded intermediate classes
        for intermediate in get_loaded_intermediate_classes(self.__class__):
            private_member_name = get_private_name(getattr(intermediate, INTERMEDIATE_NAME))
            if not hasattr(self, private_member_name):
                logger.debug("Instantiating intermediate class %s as %s", intermediate, private_member_name)
                instance = intermediate(self, *args, **kwargs)  # pass self as parent
                self._add_intermediate_instance(private_member_name, instance)

    def _add_intermediate_instance(self, private_member_name, instance):
        logger.debug("Adding intermediate class instance as member %s", private_member_name)
        setattr(self, private_member_name, instance)

    def _get_root_object(self):
        """internal method to enable intermediate member class to get the original loadable class instance"""
        walker = self
        while walker._loadable_parent is not None:
            walker = walker._loadable_parent
        return walker

    def _get_root_ia_uri(self):
        return getattr(self._get_root_object(), IA_URI)


def upper_first(s):
    return '' if not s else s[0].upper() + s[1:]


def underscores_to_pascal(s):
    return '' if not s else ''.join([upper_first(s) for s in s.split('_')])


def get_loadable_class_name_from_command_prefix(command_prefix):
    parts = command_prefix.split(':')
    term = underscores_to_pascal(parts[0])
    if len(parts) == 1:
        return "_Base" + term
    else:
        return underscores_to_pascal(parts[1]) + term


def get_base_class_name_from_prefix(command_prefix):
    parts = command_prefix.split(':')
    term = underscores_to_pascal(parts[0])
    if len(parts) == 1:
        return CommandLoadable.__name__
    return "_Base" + term


def get_loadable_class_from_name(class_name, command_prefix):
    from intelanalytics.core.api import api_globals
    import inspect
    for item in api_globals:
        if inspect.isclass(item) and item.__name__ == class_name:
            return item
    base_class_name = get_base_class_name_from_prefix(command_prefix)
    base_class = CommandLoadable if base_class_name == CommandLoadable.__name__\
        else get_loadable_class_from_name(base_class_name, command_prefix)
    loadable_class = create_loadable_class(class_name, base_class, api_globals, "", command_prefix)
    api_globals.add(loadable_class)
    return loadable_class


def get_loadable_class_from_command_prefix(command_prefix):
    class_name = get_loadable_class_name_from_command_prefix(command_prefix)
    loadable_class = get_loadable_class_from_name(class_name, command_prefix)
    return loadable_class


def get_loadable_class_from_command_def(command_def):
    return get_loadable_class_from_command_prefix(command_def.prefix)


def install_command_defs(command_defs):
    from intelanalytics.rest.command import execute_command
    for command_def in command_defs:
        # get class
        loadable_class = get_loadable_class_from_command_def(command_def)
        # add command def to class  (variant on load_loadable)
        load_loadable(loadable_class, command_def, execute_command)


def get_private_name(name):
    return '_' + name


def get_intermediate_class_name(parent_class, intermediate_name):
    """Returns the name for intermediate class based on its parent class and intermediate name"""
    # Example.   get_intermediate_class_name(TitanGraph, 'ml') returns 'TitanGraphMl'
    prefix = parent_class.__name__ if parent_class else ''
    suffix = intermediate_name[0].upper() + intermediate_name[1:]
    return prefix + suffix


def get_intermediate_class(parent_class, intermediate_name):
    """Creates and/or gets the intermediate class"""
    class_name = get_intermediate_class_name(parent_class, intermediate_name)
    # Validate that if existing member of such a name is already there, it's a getter for the loadable class we want
    if hasattr(parent_class, intermediate_name):
        prop = getattr(parent_class, intermediate_name)
        if type(prop) is property and hasattr(prop.fget, DOC_STUB):
            pass
        elif type(prop) is not property \
                or not hasattr(prop.fget, INTERMEDIATE_NAME) \
                or not getattr(prop.fget, INTERMEDIATE_NAME) == intermediate_name:
            raise ValueError("CommandLoadable Class %s already has a member %s which does not access a loadable class."
                         % (parent_class.__name__, intermediate_name))
    return _created_classes.get(class_name, None) or create_intermediate_class(parent_class, intermediate_name)


def create_intermediate_class(parent_class, intermediate_name):
    class_name = get_intermediate_class_name(parent_class, intermediate_name)
    doc = "Contains %s functionality for :class:`~intelanalytics.%s`" % (intermediate_name, parent_class.__name__)
    intermediate_class = create_loadable_class(class_name, CommandLoadable, parent_class, doc, intermediate_name)
    setattr(intermediate_class, INTERMEDIATE_NAME, intermediate_name)
    return intermediate_class


def create_loadable_class(new_class_name, base_class, namespace_obj, doc, command_prefix):
    """Dynamically create a class type with the given name and namespace_obj"""
    new_class = type(str(new_class_name),
                     (base_class,),
                     {'__doc__': doc, '__module__': namespace_obj.__module__})
    # assign to its module, and to globals
    # http://stackoverflow.com/questions/13624603/python-how-to-register-dynamic-class-in-module
    setattr(sys.modules[new_class.__module__], new_class.__name__, new_class)
    globals()[new_class.__name__] = new_class
    _created_classes[new_class.__name__] = new_class
    setattr(new_class, COMMAND_PREFIX, command_prefix)
    return new_class


def add_intermediate_class(parent_class, intermediate_name):
    """Add the class to the list of classes which the loadable class should instantiate
       during its __init__.  It will instantiate it as a private member, with a leading
       underscore character.  So this method also adds a getter property to the
       loadable_class definition"""
    intermediate_class = get_intermediate_class(parent_class, intermediate_name)

    # Add a property getter which returns an instance member variable of the
    # same name prefixed w/ an underscore, per convention
    prop = create_intermediate_property(intermediate_class, intermediate_name)
    setattr(parent_class, intermediate_name, prop)

    # store class in the parent's loaded intermediate classes attribute
    get_loaded_intermediate_classes(parent_class).add(intermediate_class)

    return intermediate_class


def check_loadable_class(cls, command_def):
    error = None
    if not hasattr(cls, COMMAND_PREFIX):
        error = "CommandLoadable inheritor %s lacks implementation for '%s'" % (cls, COMMAND_PREFIX)
    elif command_def.prefix != getattr(cls, COMMAND_PREFIX):
        error = "%s is not the class's accepted command prefix: %s"\
                % (command_def.prefix, getattr(cls, COMMAND_PREFIX))
    if error:
        raise ValueError("API load error: Class %s cannot load command_def '%s'.\n%s"
                         % (cls.__name__, command_def.full_name, error))


def load_loadable(loadable_class, command_def, execute_command_function):
    """Adds command dynamically to the loadable_class"""
    check_loadable_class(loadable_class, command_def)
    if command_def.full_name not in muted_commands:
        function = create_function(loadable_class, command_def, execute_command_function)
        # First add any intermediate member classes to provide intended scoping
        current_class = loadable_class
        for intermediate_name in command_def.intermediates:
            current_class = add_intermediate_class(current_class, intermediate_name)
        add_command(current_class, command_def, function)


def get_loaded_intermediate_classes(loadable_class):
    attr_name = loadable_class.__name__ + LOADED_INTERMEDIATE_CLASSES
    if not hasattr(loadable_class, attr_name):
        setattr(loadable_class, attr_name, set())
    return getattr(loadable_class, attr_name)


def get_loaded_commands(loadable_class):
    attr_name = loadable_class.__name__ + LOADED_COMMANDS
    if not hasattr(loadable_class, attr_name):
        setattr(loadable_class, attr_name, [])
    return getattr(loadable_class, attr_name)


def add_command(loadable_class, command_def, function):
    # Add the function if it doesn't already exist or exists as a doc_stub
    if not hasattr(loadable_class, command_def.name) or hasattr(getattr(loadable_class, command_def.name), DOC_STUB):
        setattr(loadable_class, command_def.name, function)
        get_loaded_commands(loadable_class).append(command_def)
        #print "%s <-- %s" % (loadable_class.__name__, command_def.full_name)
        logger.debug("Added function %s to class %s", command_def.name, loadable_class)


def validate_arguments(arguments, parameters):
    """
    Returns validated and possibly re-cast arguments

    Use parameter definitions to make sure the arguments conform.  This function
    is closure over in the dynamically generated execute command function
    """
    from intelanalytics.core.frame import Frame
    from intelanalytics.core.graph import Graph
    validated = {}
    for (k, v) in arguments.items():
        try:
            parameter = [p for p in parameters if p.name == k][0]
        except IndexError:
            raise ValueError("No parameter named '%s'" % k)
        if (parameter.data_type is Frame or parameter.data_type is Graph) and not isinstance(v, int):
            v = v._id  # TODO - improve this
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
    def execute_command(name, selfish, **kwargs):
        arguments = validate_arguments(kwargs, parameters)
        return execute_command_function(name, selfish, **arguments)
    return execute_command


def get_self_argument_text():
    """Produces the text for argument to use for self in a command call"""
    return "self.%s().%s" % (CommandLoadable._get_root_object.__name__, IA_URI)


def get_function_parameters_text(command_def):
    return ", ".join(['self' if param.use_self else
                      param.name if not param.optional else
                      "%s=%s" % (param.name, _default_val_to_str(param))
                      for param in command_def.parameters])


def get_function_kwargs(command_def):
    return ", ".join(["%s=%s" % (p.name, p.name if not p.use_self else get_self_argument_text())
                      for p in command_def.parameters])


def get_function_text(command_def, body_text='pass', decorator_text=''):
    """Produces python code text for a command to be inserted into python modules"""
    return '''{decorator}
def {func_name}({parameters}):
    """
    {doc}
    """
    {body_text}
'''.format(decorator=decorator_text,
           func_name=command_def.name,
           parameters=get_function_parameters_text(command_def),
           doc=command_def.doc,
           body_text=body_text)


def get_call_execute_command_text(command_def):
    return "%s('%s', self, %s)" % (EXECUTE_COMMAND_FUNCTION_NAME,
                             command_def.full_name,
                             get_function_kwargs(command_def))


def _default_val_to_str(param):
    return param.default if param.default is None or param.data_type not in [str, unicode] else "'%s'" % param.default


def create_function(loadable_class, command_def, execute_command_function=None):
    """Creates the function which will appropriately call execute_command for this command"""
    execute_command = create_execute_command_function(command_def, execute_command_function)
    func_text = get_function_text(command_def, body_text='return ' + get_call_execute_command_text(command_def), decorator_text='@api')
    try:
        func_code = compile(func_text, '<string>', "exec")
    except:
        sys.stderr.write("Metaprogramming problem compiling %s for class %s" %
                         (command_def.full_name, loadable_class.__name__))
        raise
    func_globals = {}
    api_decorator = get_api_decorator(logging.getLogger(loadable_class.__module__))
    eval(func_code, {'api': api_decorator, EXECUTE_COMMAND_FUNCTION_NAME: execute_command}, func_globals)
    function = func_globals[command_def.name]
    function.command = command_def
    function.__doc__ = command_def.doc
    return function


def get_property_text(intermediate_class):
    intermediate_name = getattr(intermediate_class, INTERMEDIATE_NAME)
    return """@property
@{doc_stub}
def {name}(self):
    \"""
    {doc}
    \"""
    return {cls}()
    """.format(doc_stub=doc_stub.__name__,
               name=intermediate_name,
               doc=_get_property_doc(intermediate_class, intermediate_name),
               cls=intermediate_class.__name__)


def mark_with_intermediate_name(obj, intermediate_name):
    setattr(obj, INTERMEDIATE_NAME, intermediate_name) # mark the getter in order to recognize name collisions


def get_fget(intermediate_name):
    private_name = get_private_name(intermediate_name)
    def fget(self):
        return getattr(self, private_name)
    return fget


def create_intermediate_property(intermediate_class, intermediate_name):
    fget = get_fget(intermediate_name)
    mark_with_intermediate_name(fget, intermediate_name)
    doc = _get_property_doc(intermediate_class, intermediate_name)
    return property(fget=fget, doc=doc)

def _get_intermediate_sphinx_link_name(intermediate_class):
    return "link_" + intermediate_class.__name__

def _get_property_doc(intermediate_class, intermediate_name):
    return "Access to object's %s functionality (See :class:`~intelanalytics.core.docstubs.%s`)" %\
           (intermediate_name, intermediate_class.__name__)

#
# doc stubs
#


class DocStubsImport(object):
    """Methods for handling import of docstubs.py"""

    @staticmethod
    def success(module_logger, class_names_str):
        module_logger.info("Doc stubs inherited from docstubs.py for %s" % class_names_str)
        import os
        if os.getenv('INTELANALYTICS_BUILD_API_DOCS', False):
            raise RuntimeError("Doc stubs were inherited during build.  This probably means"
                               "the previous docstubs .py and/or .pyc files were not deleted")

    @staticmethod
    def failure(module_logger, class_names_str, e):
        msg = "Unable to inherit doc stubs from docstubs.py for %s: %s" % (class_names_str, e)
        module_logger.warn(msg)
        #import warnings
        #warnings.warn(msg, RuntimeWarning)
        return CommandLoadable

doc_stubs_import = DocStubsImport


class DocStubCalledError(RuntimeError):
    def __init__(self, func_name=''):
        RuntimeError.__init__(self, "Call made to a documentation stub function '%s' "
                                    "which is just a placeholder for the real function."
                                    "This usually indicates a problem with a API loaded from server." % func_name)


def _doc_stub(function, *args, **kwargs):
    raise DocStubCalledError(function.__name__)


def doc_stub(function):
    """Doc stub decorator"""
    decorated_function = decorator(_doc_stub, function)
    setattr(decorated_function, DOC_STUB, function.__name__)
    return decorated_function


def api_class_alias(cls):
    """Decorates aliases (which use inheritance) to NOT have DOC STUB TEXT"""
    set_doc_stub_text(cls, None)
    return cls


def set_function_doc_stub_text(function, params_text):
    doc_stub_text = '''@{doc_stub}
def {name}({params}):
    """
    {doc}
    """
    pass'''.format(doc_stub=doc_stub.__name__,
                   name=function.__name__,
                   params=params_text,
                   doc=function.__doc__)
    set_doc_stub_text(function, doc_stub_text)


def is_class_command_loadable(cls):
    return CommandLoadable in inspect.getmro(cls)

def get_base_class_via_inspect(cls):
    return inspect.getmro(cls)[1]


def set_class_doc_stub_text(cls):
    doc_stub_text = '''@{doc_stub}
class {name}({base}):
    """
    {doc}
    """
    pass'''.format(doc_stub=doc_stub.__name__,
                   name=cls.__name__,
                   base=DOC_STUB_LOADABLE_CLASS_PREFIX + cls.__name__, #get_base_class_via_inspect(cls).__name__,
                   doc=cls.__doc__)
    set_doc_stub_text(cls, doc_stub_text)


def set_doc_stub_text(item, text):
    setattr(item, DOC_STUB_TEXT, text)


def get_doc_stub_class_text(loaded_class):
    """
   Produces code text for the base class from which the main loadable class
   will inherit the commands --i.e. the main class of the doc stub *.py file
   """
    members_text = get_members_text(loaded_class)
    class_text = get_loadable_class_text(get_doc_stubs_class_name(loaded_class),
                                     "Contains commands for %s provided by the server" % loaded_class.__name__,
                                     members_text) if members_text else ''
    intermediate_classes_text = get_intermediate_classes_text(loaded_class)
    return "\n".join([class_text, intermediate_classes_text]) if class_text or intermediate_classes_text else ''


def get_doc_stub_globals_text(module):
    doc_stub_all = []
    lines = []
    for key, value in sorted(module.__dict__.items()):
        if hasattr(value, DOC_STUB_TEXT):
            doc_stub_text = getattr(value, DOC_STUB_TEXT)
            if doc_stub_text:
                doc_stub_all.append(key)
                lines.append(doc_stub_text)
    if doc_stub_all:
        lines.insert(0, '__all__ = ["%s"]' % '", "'.join(doc_stub_all))
    return '\n\n\n'.join(lines) if lines else ''


def get_doc_stubs_module_text(command_defs, existing_loadables_dict, global_module):
    for command_def in command_defs:
        class_name = get_loadable_class_name_from_command_prefix(command_def.prefix)
        if class_name not in existing_loadables_dict:
            cls = get_loadable_class_from_command_def(command_def)
            existing_loadables_dict[class_name] = cls
        loadable_class = existing_loadables_dict[class_name]
        load_loadable(loadable_class, command_def, None)  # None for execute_command, since this is a doc stub
    lines = [get_doc_stub_class_text(loaded_class) for loaded_class in existing_loadables_dict.values()]
    lines.append(get_doc_stub_globals_text(global_module))
    for line in lines:
        if line:
            lines.insert(0, get_file_header_text())
            return '\n'.join(lines)
    return ''


def get_file_header_text():
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

# Auto-generated file for API static documentation stubs ({timestamp})
#
# **DO NOT EDIT**

from {module} import {objects}

""".format(timestamp=datetime.datetime.now().isoformat(),
           module=__name__,
           objects=", ".join([CommandLoadable.__name__, doc_stub.__name__]))


def get_loadable_class_text(class_name, doc, members_text):
    """
    Produces code text for a loadable class definition
    """
    return '''
class {name}({base_class}):
    """
{doc}
    """

    def __init__(self, *args, **kwargs):
        {base_class}.__init__(self, *args, **kwargs)

{members}
'''.format(name=class_name, base_class=CommandLoadable.__name__, doc=indent(doc), members=members_text)


def get_doc_stubs_class_name(loaded_class):
    return DOC_STUB_LOADABLE_CLASS_PREFIX + loaded_class.__name__


def get_loaded_base_class_text(loaded_class):
    """
    Produces code text for the base class from which the main loadable class
    will inherit the commands --i.e. the main class of the doc stub *.py file
    """
    members_text = get_members_text(loaded_class)
    return get_loadable_class_text(get_doc_stubs_class_name(loaded_class),
                                   "Contains commands for %s provided by the server" % loaded_class.__name__,
                                   members_text) if members_text else ''


def get_intermediate_classes_text(loaded_class):
    """
    Produces code text for dynamically created loadable classes needed as intermediate objects
    """
    lines = []
    q = deque(get_loaded_intermediate_classes(loaded_class))
    while len(q):
        c = q.pop()
        lines.append(get_loadable_class_text(c.__name__, c.__doc__, get_members_text(c)))
        q.extendleft(get_loaded_intermediate_classes(c))
    return "\n".join(lines)


def get_members_text(loaded_class):
    """
    Produces code text for all the commands (both functions and properties)
    that have been loaded into the loadable class
    """
    lines = []
    for command in get_loaded_commands(loaded_class):
        lines.append(indent(get_function_text(command, decorator_text='@' + doc_stub.__name__)))
    for intermediate_class in get_loaded_intermediate_classes(loaded_class):
        lines.append(indent(get_property_text(intermediate_class)))
    return "\n".join(lines)


def indent(text, spaces=4):
    indentation = ' ' * spaces
    return "\n".join([indentation + line for line in text.split('\n')])


def delete_docstubs():
    """
    Deletes all the doc_stub functions from all classes in docstubs.py
    """
    try:
        import intelanalytics.core.docstubs as docstubs
    except Exception:
        logger.info("No docstubs.py found, nothing to delete")
    else:
        for item in docstubs.__dict__.values():
            if inspect.isclass(item):
                victims = [k for k, v in item.__dict__.iteritems() if hasattr(v, '__call__') and hasattr(v, DOC_STUB)]
                logger.debug("deleting docstubs from %s: %s", item, victims)
                for victim in victims:
                    delattr(item, victim)
