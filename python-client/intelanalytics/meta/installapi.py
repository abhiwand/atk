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
Install API methods, and other API utils
"""

import inspect
import logging
logger = logging.getLogger('meta')

from intelanalytics.core.api import api_status, api_globals
from intelanalytics.meta.docstub import delete_docstubs, is_doc_stub
from intelanalytics.meta.clientside import client_commands, clear_clientside_api_stubs, is_api
from intelanalytics.meta.mute import muted_commands
import intelanalytics.meta.metaprog as metaprog


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


def install_client_commands():
    cleared_classes = set()
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
            cls_with_installation = metaprog.get_class_from_store(metaprog.InstallPath(command_def.entity_type))
            if cls_with_installation is not cls:
                raise RuntimeError("Internal Error: @api decoration resolved with mismatched classes for %s" % class_name)
            if cls not in cleared_classes:
                clear_clientside_api_stubs(cls)
                cleared_classes.add(cls)
            installation = metaprog.get_installation(cls_with_installation)
            if command_def.entity_type != installation.install_path.full:
                raise RuntimeError("Internal Error: @api decoration resulted in different install paths '%s' and '%s' for function %s in class %s"
                                   % (command_def.entity_type, installation.install_path, command_def.name, class_name))

            if command_def.full_name not in muted_commands:
                installation.commands.append(command_def)
                setattr(cls, command_def.name, command_def.client_member)
                logger.debug("Installed client-side api function %s to class %s", command_def.name, cls)

        elif command_def.client_member not in api_globals:
            # global function
            api_globals.add(command_def.client_member)


def install_command_def(cls, command_def, execute_command_function):
    """Adds command dynamically to the loadable_class"""
    if command_def.full_name not in muted_commands:
        if is_command_name_installable(cls, command_def.name):
            check_loadable_class(cls, command_def)
            function = metaprog.create_function(cls, command_def, execute_command_function)
            function._is_api = True
            if command_def.is_constructor:
                cls.__init__ = function
                cls.__repr__ = metaprog.get_repr_function(command_def)
            else:
                setattr(cls, command_def.name, function)
            metaprog.get_installation(cls).commands.append(command_def)
            #print "%s <-- %s" % (cls.__name__, command_def.full_name)
            logger.debug("Installed api function %s to class %s", command_def.name, cls)


def handle_constructor_command_defs(constructor_command_defs):
    for d in sorted(constructor_command_defs, key=lambda x: len(x.full_name)):  # sort so base classes are created first
        metaprog.create_entity_class(d)


def is_command_name_installable(cls, command_name):
    # name doesn't already exist or exists as a doc_stub
    return not hasattr(cls, command_name) or is_doc_stub(getattr(cls, command_name))


def check_loadable_class(cls, command_def):

    error = None
    installation = metaprog.get_installation(cls)
    if not installation:
        error = "class %s is not prepared for command installation" % cls
    elif command_def.is_constructor and not installation.install_path.is_entity:
        raise RuntimeError("API load error: special method '%s' may only be defined on entity classes, not on entity "
                           "member classes (i.e. only one slash allowed)." % command_def.full_name)
    elif command_def.entity_type != installation.install_path.entity_type:
        error = "%s is not the class's accepted command entity_type: %s" \
                % (command_def.entity_type, installation.install_path.entity_type)
    if error:
        raise ValueError("API load error: Class %s cannot load command_def '%s'.\n%s"
                         % (cls.__name__, command_def.full_name, error))


def install_server_commands(command_defs):
    from intelanalytics.rest.command import execute_command

    # Unfortunately we need special logic to handle command_defs which define constructors
    # for entity classes.  We must install the constructor command_defs first, such that the
    # appropriate entity classes get built knowing their docstring (because __doc__ on a type
    # is readonly in Python --the real problem), instead of being built generically as
    # dependencies of regular command_defs.
    handle_constructor_command_defs([d for d in command_defs if d.is_constructor])

    for command_def in command_defs:
        cls = metaprog.get_or_create_class(command_def.install_path)
        install_command_def(cls, command_def, execute_command)

    post_install_clean_up()


def post_install_clean_up():
    """Do some post-processing on the installation to clean things like property inheritance"""

    # Find all the intermediate properties that are using a base class and create an appropriate
    # inherited class that matches the inheriance of its parent class
    # Example:  say someone registers   "graph/colors/red"
    # We end up with  _BaseGraphColors, where _BaseGraph has a property 'colors' which points to it
    # Then we see  "graph:titan/colors/blue"
    # We get TitanGraphColors(_BaseGraphColors) and TitaGraph has a property 'colors' which points to it
    # Now, regular old Graph(_BaseGraph) did get any commands installed at colors specifically, so it inherits property 'color' which points to _BaseGraphColors
    # This makes things awkward because the Graph class installation doesn't know about the property color, since it wasn't installed there.
    # It becomes much more straightforward in Graph gets its own proeprty 'colors' (overriding inheritance) which points to a class GraphColors
    # So in this method, we look for this situation, now that installation is complete.
    installable_classes = metaprog.get_installable_classes()
    for cls in installable_classes:
        installation = metaprog.get_installation(cls)
        if installation:
            for name in dir(cls):
                member = getattr(cls, name)
                if metaprog.is_intermediate_property(member):
                    if name not in cls.__dict__:  # is inherited...
                        path =metaprog.InstallPath(installation.install_path.entity_type + '/' + name)
                        new_class = metaprog._create_class(path)  # create new, appropriate class to avoid inheritance
                        installation.add_property(cls, name, new_class)
                        metaprog.get_installation(new_class).keep_me = True  # mark its installation so it will survive the next for loop
            # Some added properties may access intermediate classes which did not end up
            # receiving any commands. They are empty and should not appear in the API.  We
            # will take a moment to delete them.  This approach seemed much better than writing
            # special logic to calculate fancy inheritance and awkwardly insert classes into
            # the hierarchy on-demand.
            for name, intermediate_cls in installation.intermediates.items():
                intermediate_installation = metaprog.get_installation(intermediate_cls)
                if not intermediate_installation.commands and not hasattr(intermediate_installation, "keep_me"):
                    delattr(cls, name)
                    del installation.intermediates[name]


def install_api():

    """
    Download API information from the server, once.

    After the API has been loaded, it cannot be changed or refreshed.  Also, server connection
    information cannot change.  User must restart Python in order to change connection info.

    Subsequent calls to this method invoke no action.
    """
    if not api_status.is_installed:
        server_commands = download_server_commands()

        from intelanalytics.rest.iaserver import server
        from intelanalytics.rest.jsonschema import get_command_def
        #try:
        delete_docstubs()
        install_client_commands()  # first do the client-side specific processing
        install_server_commands(server_commands)
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


def walk_api(cls, cls_function, attr_function, include_init=False):
    """
    Walks the API and executes the cls_function at every class and the attr_function at every attr

    Skip all private methods.  If __init__ should be included (special case), set include_init=True

    Either *_function arg can be None.  For example if you're only interesting in walking functions,
    set cls_function=None.
    """
    cls_func = cls_function
    attr_func = attr_function
    inc_init = include_init

    def walker(obj):
        for name in dir(obj):
            if not name.startswith("_") or (inc_init and name == "__init__"):
                a = getattr(obj, name)
                if inspect.isclass(a):
                    if is_api(a) and cls_func:
                        cls_func(a)
                    walker(a)
                else:
                    if isinstance(a, property):
                        intermediate_class = metaprog.get_intermediate_class(name, obj)
                        if intermediate_class:
                            walker(intermediate_class)
                        a = a.fget
                    if is_api(a) and attr_func:
                        attr_func(obj, a)
    walker(cls)


def get_api_names(obj, prefix=''):
    """
    Collects the full names of the commands in the API (driven by QA coverage)
    """
    names = set()

    def collect(o, a):
        try:
            if not metaprog.is_intermediate_property(a):
                name = a.command.full_name
                if name not in names:
                    names.add(name)
        except:
            pass

    walk_api(obj, None, collect, include_init=True)
    return names
