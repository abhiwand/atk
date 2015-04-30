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
Meta-programming - dynamically adding commands to api objects or building doc stub *.py
"""

#  Example: given commands "graph:titan/ml/pagerank" and "graph/ml/graphx_pagerank"  and class structure:
#
#     CommandsInstallable
#            |
#       _BaseGraph
#       /         \
#    Graph       TitanGraph
#
#
#  We need to create:
#
#     _BaseGraphMl defines "graphx_pagerank"
#       /         \
#    GraphMl       TitanGraphMl defines "pagerank"
#
# such that
#
# t = TitanGraph()
# t.ml.graphx_pagerank(...)   # works
# t.ml.pagerank(...)          # works
# g = Graph()
# g.ml.graphx_pagerank(...)   # works
# g.ml.pagerank(...)          # attribute 'pagerank' not found (desired behavior)
from intelanalytics.meta.installpath import InstallPath
from intelanalytics.core.iatypes import valid_data_types


__all__ = ['CommandInstallable', 'load_loadable']


import logging
logger = logging.getLogger(__name__)

import sys
import datetime
import inspect
from decorator import decorator

from intelanalytics.core.api import api_globals
from intelanalytics.meta.context import get_api_context_decorator
from intelanalytics.meta.mute import muted_commands
import intelanalytics.meta.classnames as naming

_installable_classes_store = {}
_docstub_classes_store = {}


class CommandInstallation(object):
    """
    Object that gets installed into a CommandInstallable class, as a private member, to hold command installation info.
    """

    def __init__(self, install_path, host_class_was_created):
        if not isinstance(install_path, InstallPath):
            install_path = InstallPath(install_path)
        self.install_path = install_path  # the path of which the installation's class is target
        self.host_class_was_created = host_class_was_created
        self.commands = []  # list of command definitions
        self.intermediates = {}  # property name --> intermediate class

    def add_property(self, parent_class, property_name, intermediate_class):
        if get_installation(parent_class) != self:
            raise RuntimeError("Internal error: installation and class type mismatch for class %s" % parent_class)
        if property_name not in parent_class.__dict__:   # don't use hasattr, because it would match an inherited prop
            setattr(parent_class, property_name, self._create_intermediate_property(property_name, intermediate_class))
            self.intermediates[property_name] = intermediate_class

    def get_intermediate_class(self, property_name):
        return self.intermediates.get(property_name, None)

    @staticmethod
    def _create_intermediate_property(name, intermediate_class):
        fget = CommandInstallation.get_fget(name)
        doc = CommandInstallation._get_canned_property_doc(name, intermediate_class.__name__)
        return property(fget=fget, doc=doc)

    @staticmethod
    def get_fget(name):
        private_name = naming.name_to_private(name)

        def fget(self):
            return getattr(self, private_name)
        return fget

    @staticmethod
    def _get_canned_property_doc(name, class_name):
        return "Access to object's %s functionality (See :class:`~intelanalytics.core.docstubs.%s`)" % (name, class_name)


def doc_stub(function):
    """Doc stub decorator"""
    decorated_function = decorator(_doc_stub, function)
    setattr(decorated_function, Constants.DOC_STUB, function.__name__)
    return decorated_function


class Constants(object):
    IA_URI = '_id'
    COMMAND_INSTALLATION = "_command_installation"

    INTERMEDIATE_CLASS = '_intermediate_class'
    LOADED_COMMANDS = '_loaded_commands'
    LOADED_INTERMEDIATE_CLASSES = '_loaded_intermediate_classes'
    INIT_COMMAND = "_init_command"  # attribute for a class to hold the command_def of its init function, since the __init__ function is immutable


    EXECUTE_COMMAND_FUNCTION_NAME = 'execute_command'
    ALIASED_EXECUTE_COMMAND_FUNCTION_NAME = 'aliased_execute_command'

    INIT_INFO_ARGUMENT_NAME = '_info'

    DOC_STUB = 'doc_stub'
    DOC_STUB_LOADABLE_CLASS_PREFIX = '_DocStubs'
    DOC_STUB_TEXT = '_doc_stub_text'  # attribute for a function to hold on to its own doc stub text
    DOC_STUB_DECORATOR_TEXT = '@' + doc_stub.__name__


class CommandNotLoadedError(NotImplementedError):
    pass


class CommandInstallable(object):
    """
    Base class for objects which accept dynamically created members based on external info

    i.e. the class have commands installed dynamically

    Inheritors must...

    1.  Implement the following:
        instance attribute '_id' : str or int  (See IA_URI constant)
            identifies the instance to the server

    2.  Call CommandsInstallable.__init__(self) in its own __init__, IMMEDIATELY
    """

    def __init__(self, entity=None):
        logger.debug("Enter CommandsInstallable.__init__ from class %s" % self.__class__)
        # By convention, the entity instance is passed as the first arg for initializing intermediate classes.  This
        # is internal to the metaprogramming.  Standard entity class init is just called with only self.
        self._entity = entity if entity else self

        # Instantiate intermediate classes for the properties which scope installed commands.
        # To honor inheritance overriding, we must start at the most base class (not inc. CommandInstallable)
        # and work towards self's class, doing it last.
        class_lineage = inspect.getmro(self.__class__)
        index = class_lineage.index(CommandInstallable)
        for i in reversed(xrange(index)):
            cls = class_lineage[i]
            if has_installation(cls):
                self._init_intermediate_classes(cls)

    def _init_intermediate_classes(self, installation_class):
            """Instantiates every intermediate class defined in the installation class and adds
            them as private members to the new_instance, such that the properties (already defined)
            have something to return"""
            installation = get_installation(installation_class)
            for name, cls in installation.intermediates.items():
                private_member_name = naming.name_to_private(name)
                if not hasattr(self, private_member_name):
                    logger.debug("Instantiating intermediate class %s as %s", cls, private_member_name)
                    private_member_value = cls(self._entity)  # instantiate
                    logger.debug("Adding intermediate class instance as member %s", private_member_name)
                    setattr(self, private_member_name, private_member_value)

    def _get_entity_ia_uri(self):
        """standard way for generated code to ask for the entity id"""
        return getattr(self._entity, Constants.IA_URI)


def is_class_command_installable(cls):
    return CommandInstallable in inspect.getmro(cls)


def has_installation(cls):
    """tests if the given class type itself has a command installation object (and not inherited from a base class)"""
    return Constants.COMMAND_INSTALLATION in cls.__dict__  # don't use hasattr


def get_installation(cls, *default):
    """returns the installation obj for given class type

    :param cls:
    :return: CommandInstallation
    """
    try:
        return cls.__dict__[Constants.COMMAND_INSTALLATION]  # don't use getattr
    except KeyError:
        if len(default) > 0:
            return default[0]
        raise AttributeError("Class %s does not have a command installation object" % cls)


def set_installation(cls, installation):
    """makes the installation object a member of the class type"""
    setattr(cls, Constants.COMMAND_INSTALLATION, installation)
    _installable_classes_store[installation.install_path.full] = cls  # update store


def get_class_from_store(install_path):
    """tries to find a class for the install_path in the store (or global API), returns None if not found"""
    cls = _installable_classes_store.get(install_path.full, None)
    if not cls:
        # see if cls is in the global API already and just needs to be added to the store by adding an installation obj
        class_name, baseclass_name = install_path.get_class_and_baseclass_names()
        for item in api_globals:
            if inspect.isclass(item) and item.__name__ == class_name:
                if not is_class_command_installable(item):
                    raise RuntimeError("Global class %s does not inherit %s, unable to install command with path '%s'" %
                                       (class_name, CommandInstallable.__name__, install_path.full))
                set_installation(item, CommandInstallation(install_path, host_class_was_created=False))
                return item
    return cls


def get_entity_class_from_store(entity_type):
    return get_class_from_store(InstallPath(entity_type))


def get_class(install_path):
    return get_class_from_store(install_path) or create_classes(install_path)


def _create_class(install_path, doc=None):
    """helper method which creates a single class for the given install path"""
    new_class_name, baseclass_name = install_path.get_class_and_baseclass_names()
    baseclass_install_path = install_path.baseclass_install_path
    #print "baseclass_install_path=%s" % baseclass_install_path
    if baseclass_name == CommandInstallable.__name__ or baseclass_install_path == install_path:
        baseclass = CommandInstallable
    else:
        baseclass = get_class(baseclass_install_path)
    new_class = _create_class_type(new_class_name,
                                   baseclass,
                                   doc=str(doc) or install_path.get_generic_doc_str(),
                                   init=get_class_init_from_path(install_path))
    set_installation(new_class, CommandInstallation(install_path, host_class_was_created=True))
    return new_class


def create_classes(install_path):
    """creates all the necessary classes to enable the given install path"""
    new_class = None
    parent_class = None
    for path in install_path.gen_composite_install_paths:
        path_class = get_class_from_store(path)
        if not path_class:
            new_class = _create_class(path)
            if parent_class is not None:
                get_installation(parent_class).add_property(parent_class, path.property_name, new_class)
            path_class = new_class
        parent_class = path_class
    if new_class is None:
        raise RuntimeError("Internal Error: algo was not expecting new_class to be None")
    return new_class


def create_entity_class(command_def):
    if not command_def.is_constructor:
        raise RuntimeError("Internal Error: algo was not a constructor command_def")
    new_class = _create_class(command_def.install_path, command_def.doc)
    from intelanalytics.meta.clientside import decorate_api_class
    decorate_api_class(new_class)


def get_default_init():

    def init(self, entity):
        CommandInstallable.__init__(self, entity)
    return init


def _create_class_type(new_class_name, baseclass, doc, init=None):
    """Dynamically create a class type with the given name and namespace_obj"""
    if logger.level == logging.DEBUG:
        logger.debug("_create_class_type(new_class_name='%s', baseclass=%s, doc='%s', init=%s)",
                     new_class_name,
                     baseclass,
                     "None" if doc is None else "%s..." % doc[:12],
                     init)
    new_class = type(str(new_class_name),
                     (baseclass,),
                     {'__init__': init or get_default_init(),
                      '__doc__': doc,
                      '__module__': api_status.__module__})
    # assign to its module, and to globals
    # http://stackoverflow.com/questions/13624603/python-how-to-register-dynamic-class-in-module
    setattr(sys.modules[new_class.__module__], new_class.__name__, new_class)
    globals()[new_class.__name__] = new_class
    new_class._is_api = True
    return new_class


def is_command_name_installable(cls, command_name):
    # name doesn't already exist or exists as a doc_stub
    return not hasattr(cls, command_name) or hasattr(getattr(cls, command_name), Constants.DOC_STUB)


def install_client_commands():
    from intelanalytics.meta.clientside import client_commands
    for class_name, command_def in client_commands:

        # what to do with global methods???

        # validation
        cls = None
        if class_name:
            try:
                cls = [c for c in api_globals if inspect.isclass(c) and c.__name__ == class_name][0]
            except IndexError:
                #raise RuntimeError("Internal Error: @api decoration cannot resolve class name %s for function %s" % (class_name, command_def.name))
                pass
        if cls:
            cls_with_installation = get_class_from_store(InstallPath(command_def.entity_type))
            if cls_with_installation is not cls:
                raise RuntimeError("Internal Error: @api decoration resolved with mismatched classes for %s" % class_name)
            installation = get_installation(cls_with_installation)
            if command_def.entity_type != installation.install_path.full:
                raise RuntimeError("Internal Error: @api decoration resulted in different install paths '%s' and '%s' for function %s in class %s"
                                   % (command_def.entity_type, installation.install_path, command_def.name, class_name))

            if command_def.full_name not in muted_commands:
                installation.commands.append(command_def)
                logger.debug("Installed client-side api function %s to class %s", command_def.name, cls)

        elif command_def.client_function not in api_globals:
            # global function
            api_globals.add(command_def.client_function)


def install_command_def(cls, command_def, execute_command_function):
    """Adds command dynamically to the loadable_class"""
    if command_def.full_name not in muted_commands:
        if is_command_name_installable(cls, command_def.name):
            check_loadable_class(cls, command_def)
            function = create_function(cls, command_def, execute_command_function)
            function._is_api = True
            if command_def.is_constructor:
                cls.__init__ = function
                cls.__repr__ = get_repr(command_def)
            else:
                setattr(cls, command_def.name, function)
            get_installation(cls).commands.append(command_def)
            #print "%s <-- %s" % (cls.__name__, command_def.full_name)
            logger.debug("Installed api function %s to class %s", command_def.name, cls)


def handle_constructor_command_defs(constructor_command_defs):
    for d in sorted(constructor_command_defs, key=lambda x: len(x.full_name)):  # sort so base classes are created first
        create_entity_class(d)


def install_server_commands(command_defs):
    from intelanalytics.rest.command import execute_command

    # Unfortunately we need special logic to handle command_defs which define constructors
    # for entity classes.  We must install the constructor command_defs first, such that the
    # appropriate entity classes get built knowing their docstring (because __doc__ on a type
    # is readonly in Python --the real problem), instead of being built generically as
    # dependencies of regular command_defs.
    handle_constructor_command_defs([d for d in command_defs if d.is_constructor])

    for command_def in command_defs:
        cls = get_class(command_def.install_path)
        install_command_def(cls, command_def, execute_command)

    # Some added properties may access intermediate classes which did not end up
    # receiving any commands. They are empty and should not appear in the API.  We
    # will take a moment to delete them.  This approach seemed much better than writing
    # special logic to calculate fancy inheritance and awkwardly insert classes into
    # the hierarchy on-demand.
    del_empty_intermediate_properties()


def del_empty_intermediate_properties():
    for cls in _installable_classes_store.values():
        installation = get_installation(cls)
        for name, intermediate_cls in installation.intermediates.items():
            intermediate_installation = get_installation(intermediate_cls)
            if not intermediate_installation.commands:  # i.e. no commands have been installed
                delattr(cls, name)
                del installation.intermediates[name]


def get_class_init_from_path(install_path):
    if install_path.is_entity:
        # Means a class is being created for an entity and requires more information about the __init__ method
        # We return an __init__ that will throw an error when called.  It must be overwritten by a special plugin
        from intelanalytics.meta.command import ENTITY_CONSTRUCTOR_COMMAND_RESERVED_NAME
        plugin_name = "%s/%s" % (install_path, ENTITY_CONSTRUCTOR_COMMAND_RESERVED_NAME)
        msg = "Internal Error: server does not know how to construct entity type %s.  A command plugin " \
              "named \"%s\" must be registered." % (install_path.entity_type, plugin_name)

        def insufficient_init(self, *args, **kwargs):
            raise RuntimeError(msg)
        return insufficient_init

    def init(self, entity):
        CommandInstallable.__init__(self, entity)
    return init


def check_loadable_class(cls, command_def):
    error = None
    installation = get_installation(cls)
    if not installation:
        error = "class %s is not prepared for command installation" % cls
    elif command_def.is_constructor and not installation.install_path.is_entity:
        raise RuntimeError("API load error: special method '%s' may only be defined on entity classes, not on entity "
                           "member classes (i.e. only one slash allowed)." % command_def.full_name)
    elif command_def.entity_type != installation.install_path.entity_type:
        error = "%s is not the class's accepted command entity_type: %s"\
                % (command_def.entity_type, installation.install_path.entity_type)
    if error:
        raise ValueError("API load error: Class %s cannot load command_def '%s'.\n%s"
                         % (cls.__name__, command_def.full_name, error))


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
        if hasattr(v, "_id"):
            v = v._id
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
    def execute_command(_command_name, _selfish, **kwargs):
        arguments = validate_arguments(kwargs, parameters)
        return execute_command_function(_command_name, _selfish, **arguments)
    return execute_command


def get_self_argument_text():
    """Produces the text for argument to use for self in a command call"""
    return "self._entity.%s" % Constants.IA_URI


def get_function_kwargs(command_def):
    return ", ".join(["%s=%s" % (p.name, p.name if not p.use_self else get_self_argument_text())
                      for p in command_def.parameters])


def get_function_text(command_def, body_text='pass', decorator_text=''):
    """Produces python code text for a command to be inserted into python modules"""

    from intelanalytics.meta.genspa import gen_spa
    parameters_text=command_def.get_function_parameters_text()
    if command_def.is_constructor:
        parameters_text += ", _info=None"
    return '''{decorator}
def {func_name}({parameters}):
    """
{doc}
    """
    {body_text}
'''.format(decorator=decorator_text,
           func_name=command_def.name,
           parameters=parameters_text,
           doc=gen_spa(command_def),
           body_text=body_text)


def get_doc_stub_init_text(command_def):
    from intelanalytics.meta.genspa import gen_spa
    parameters_text=command_def.get_function_parameters_text()
    return '''
def __init__({parameters}):
    """
    {doc}
    """
    pass
'''.format(parameters=parameters_text, doc=gen_spa(command_def))


def get_call_execute_command_text(command_def):
    return "%s('%s', self, %s)" % (Constants.EXECUTE_COMMAND_FUNCTION_NAME,
                               command_def.full_name,
                               get_function_kwargs(command_def))


def get_repr(command_def):
    collection_name = naming.entity_type_to_collection_name(command_def.entity_type)
    repr_func = default_repr
    def _repr(self):
        return repr_func(self, collection_name)
    return _repr


def default_repr(self, collection_name):
    entity = type(self).__name__
    try:
        from intelanalytics.rest.connection import http
        uri = http.create_full_uri(collection_name + "/" + str(self._id))
        response = http.get_full_uri(uri).json()
        name = response.get('name', None)
        if name:
            details = ' "%s"' % response['name']
        else:
            details = ' <unnamed@%s>' % response['id']
    except:
        raise
        #details = " (Unable to collect details from server)"
    return entity + details


def get_init_text(command_def):
    return '''
    self._id = 0
    base_class.__init__(self)
    if {info} is None:
        {info} = {call_execute_create}
    # initialize from _info
    self._id = _info['id']
    self._name = _info.get('name', None)  # todo: remove, move to initialize_from_info
    # initialize_from_info(self, {info})  todo: implement
    '''.format(info=Constants.INIT_INFO_ARGUMENT_NAME,
               call_execute_create=get_call_execute_command_text(command_def))



def compile_function(func_name, func_text, dependencies):
    func_code = compile(func_text, '<string>', "exec")
    func_globals = {}
    eval(func_code, dependencies, func_globals)
    return func_globals[func_name]


def create_function(loadable_class, command_def, execute_command_function=None):
    """Creates the function which will appropriately call execute_command for this command"""
    execute_command = create_execute_command_function(command_def, execute_command_function)
    if command_def.is_constructor:
        func_text = get_function_text(command_def, body_text=get_init_text(command_def))
        #print "func_text for %s = %s" % (command_def.full_name, func_text)
        dependencies = {'base_class': _installable_classes_store.get(naming.entity_type_to_baseclass_name(command_def.install_path.full), CommandInstallable), Constants.EXECUTE_COMMAND_FUNCTION_NAME: execute_command}
    else:
        func_text = get_function_text(command_def, body_text='return ' + get_call_execute_command_text(command_def), decorator_text='@api')
        api_decorator = get_api_context_decorator(logging.getLogger(loadable_class.__module__))
        dependencies = {'api': api_decorator, Constants.EXECUTE_COMMAND_FUNCTION_NAME: execute_command}
    try:
        function = compile_function(command_def.name, func_text, dependencies)
    except:
        sys.stderr.write("Metaprogramming problem compiling %s for class %s in code: %s" %
                         (command_def.full_name, loadable_class.__name__, func_text))
        raise
    function.command = command_def
    function.__doc__ = command_def.doc
    return function


def get_doc_stub_property_text(name, class_name):
    return """@property
@{doc_stub}
def {name}(self):
    \"""
    {doc}
    \"""
    return {cls}()
    #raise RuntimeError("API error, trying to access a property written for documentation")
    """.format(doc_stub=doc_stub.__name__,
               name=name,
               doc=CommandInstallation._get_canned_property_doc(name, class_name),
               cls=class_name)


def _get_intermediate_sphinx_link_name(intermediate_class):
    return "link_" + intermediate_class.__name__

#
# doc stubs   (blbarker 4/29/15 -> broken; move elsewhere and fix as part of enabling SPA)
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
        return CommandInstallable

doc_stubs_import = DocStubsImport


class DocStubCalledError(RuntimeError):
    def __init__(self, func_name=''):
        RuntimeError.__init__(self, "Call made to a documentation stub function '%s' "
                                    "which is just a placeholder for the real function."
                                    "This usually indicates a problem with a API loaded from server." % func_name)


def _doc_stub(function, *args, **kwargs):
    raise DocStubCalledError(function.__name__)


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
                   base=Constants.DOC_STUB_LOADABLE_CLASS_PREFIX + cls.__name__, #get_base_class_via_inspect(cls).__name__,
                   doc=cls.__doc__)
    set_doc_stub_text(cls, doc_stub_text)


def set_doc_stub_text(item, text):
    setattr(item, Constants.DOC_STUB_TEXT, text)


def get_doc_stub_class_text(loaded_class):
    """
    Produces code text for the base class from which the main loadable class
    will inherit the commands --i.e. the main class of the doc stub *.py file
    """
    if not has_installation(loaded_class):
        return ''

    members_text = get_members_text(loaded_class) or indent("pass")
    installation = get_installation(loaded_class)
    class_name, baseclass_name = installation.install_path.get_class_and_baseclass_names()
    if class_name != loaded_class.__name__:
        raise RuntimeError("Internal Error: class name mismatch generating docstubs (%s != %s)" % (class_name, loaded_class.__name__))
    if not installation.host_class_was_created:
        class_name = get_doc_stubs_class_name(class_name)
        if baseclass_name != CommandInstallable.__name__:
            baseclass_name = get_doc_stubs_class_name(baseclass_name)
    class_text = get_loadable_class_text(class_name,
                                         baseclass_name,
                                         "Contains commands for %s provided by the server" % class_name,
                                         members_text)
    return class_text


def get_doc_stub_globals_text(module):
    doc_stub_all = []
    lines = []
    for key, value in sorted(module.__dict__.items()):
        if hasattr(value, Constants.DOC_STUB_TEXT):
            doc_stub_text = getattr(value, Constants.DOC_STUB_TEXT)
            if doc_stub_text:
                doc_stub_all.append(key)
                lines.append(doc_stub_text)
    if doc_stub_all:
        lines.insert(0, '__all__ = ["%s"]' % '", "'.join(doc_stub_all))
    return '\n\n\n'.join(lines) if lines else ''


def get_doc_stubs_modules_text(command_defs, global_module):
    install_server_commands(command_defs)
    delete_docstubs()
    before_lines = [get_file_header_text()]
    after_lines = []
    after_all = []
    after_definitions = []
    after_dependencies = {}
    classes = sorted(_installable_classes_store.items(), key=lambda kvp: len(kvp[0]))
    if 0 < logger.level <= logging.INFO:
        logger.info("Processing class in this order: " + "\n".join([str(c[1]) for c in classes]))

    for cls_pair in classes:
        cls = cls_pair[1]
        installation = get_installation(cls)
        members_text = get_members_text(cls) or indent("pass")
        class_name, baseclass_name = installation.install_path.get_class_and_baseclass_names()
        if class_name != cls.__name__:
            raise RuntimeError("Internal Error: class name mismatch generating docstubs (%s != %s)" % (class_name, cls.__name__))
        if installation.host_class_was_created and installation.install_path.is_entity:
            lines = after_lines
            after_definitions.append(class_name)
            after_dependencies[baseclass_name] = installation.install_path.baseclass_install_path
            #if installation.install_path.is_entity:
                # need to export it as part of __all__
            after_all.append(class_name)
        else:
            if not installation.host_class_was_created:
                class_name = get_doc_stubs_class_name(class_name)
                if baseclass_name != CommandInstallable.__name__:
                    baseclass_name = get_doc_stubs_class_name(baseclass_name)
            lines = before_lines

        lines.append(get_loadable_class_text(class_name,
                                             baseclass_name,
                                             "Contains commands for %s provided by the server" % class_name,
                                             members_text))

    after_lines.insert(0, '__all__ = ["%s"]' % '", "'.join(after_all))  # export the entities created in 'after'
    for d in after_definitions:
        after_dependencies.pop(d, None)
    for baseclass_name, install_path in after_dependencies.items():
        if install_path:
            module_path = _installable_classes_store[install_path.full].__module__
            after_lines.insert(0, "from %s import %s" % (module_path, baseclass_name))
    after_lines.insert(0, get_file_header_text())
    before_lines.append(get_doc_stub_globals_text(global_module))
    return '\n'.join(before_lines), '\n'.join(after_lines)


def get_file_header_text():
    return """##############################################################################
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

# Auto-generated file for API static documentation stubs ({timestamp})
#
# **DO NOT EDIT**

from {module} import {objects}

""".format(timestamp=datetime.datetime.now().isoformat(),
           module=__name__,
           objects=", ".join([CommandInstallable.__name__, doc_stub.__name__]))


def get_loadable_class_text(class_name, baseclass_name, doc, members_text):
    """
    Produces code text for a loadable class definition
    """
    return '''
class {name}({baseclass}):
    """
{doc}
    """

    # (the __init__ method may or may not be defined somewhere below)

{members}
'''.format(name=class_name, baseclass=baseclass_name, doc=indent(doc), members=members_text)


def get_doc_stubs_class_name(class_name):
    return Constants.DOC_STUB_LOADABLE_CLASS_PREFIX + class_name


def get_members_text(cls):
    """
    Produces code text for all the commands (both functions and properties)
    that have been loaded into the loadable class
    """
    lines = []
    installation = get_installation(cls)
    #for command in get_loaded_commands(loaded_class):
    for command in installation.commands:
        if command.is_constructor:
            lines.append(indent(get_doc_stub_init_text(command)))
        else:
            lines.append(indent(get_function_text(command, decorator_text=Constants.DOC_STUB_DECORATOR_TEXT)))
    for name, cls in installation.intermediates.items():
        lines.append(indent(get_doc_stub_property_text(name, cls.__name__)))
    return "\n".join(lines)


def indent(text, spaces=4):
    indentation = ' ' * spaces
    return "\n".join([indentation + line if line else line for line in text.split('\n')])

def get_type_name(data_type):
    try:
        return valid_data_types.to_string(data_type)
    except:
        if isinstance(data_type, basestring):
            return data_type
        return data_type.__name__ if data_type is not None else 'None'


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
                victims = [k for k, v in item.__dict__.iteritems() if hasattr(v, '__call__') and hasattr(v, Constants.DOC_STUB)]
                logger.debug("deleting docstubs from %s: %s", item, victims)
                for victim in victims:
                    delattr(item, victim)


##############################################################################
#__commands_from_backend = None

from intelanalytics.core.api import api_status


def download_server_commands():
        logger.info("Requesting available commands from server")
        from intelanalytics.rest.iaserver import server
        from intelanalytics.rest.jsonschema import get_command_def
        from intelanalytics.core.errorhandle import IaError
        try:
            response = server.get("/commands/definitions")
        except:
            import sys
            sys.stderr.write('Unable to connect to server\n')
            raise IaError(logger)

        commands_json_schema = response.json()
        # ensure the assignment to __commands_from_backend is the last line in this 'if' block before the fatal try:
        return [get_command_def(c) for c in commands_json_schema]


def install_api():

    """
    Download API information from the server, once.

    After the API has been loaded, it cannot be changed or refreshed.  Also, server connection
    information cannot change.  User must restart Python in order to change connection info.

    Subsequent calls to this method invoke no action.
    """
    if not api_status.is_installed:
        server_commands = download_server_commands()

        import traceback
        from intelanalytics.rest.iaserver import server
        from intelanalytics.rest.jsonschema import get_command_def
        from intelanalytics.core.errorhandle import errors
        #try:
        install_client_commands()  # first do the client-side specific processing
        install_server_commands(server_commands)
        delete_docstubs()
        from intelanalytics import _refresh_api_namespace
        _refresh_api_namespace()
        #except Exception as e:
        #    errors._api_load = "".join(traceback.format_exception(*sys.exc_info()))
        #    raise FatalApiLoadError(e)

        api_status.declare_installed()


class FatalApiLoadError(RuntimeError):
    def __init__(self, e):
        self.details = str(e)
        RuntimeError.__init__(self, """
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Fatal error: installing the downloaded API information failed and has left the
client in a state of unknown compatibility with the server.

Restarting your python session is now require to use this package.

Details:%s
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""" % self.details)
