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
Intel Analytics API management
"""
# import sys
# import traceback
import logging
logger = logging.getLogger(__name__)
import inspect
# from intelanalytics.core.errorhandle import IaError, errors
# from intelanalytics.core.loggers import log_api_call
# from decorator import decorator
#
# from intelanalytics.meta.classnames import class_name_to_entity_type
# from intelanalytics.meta.reflect import get_args_text_from_function, get_args_and_kwargs_from_function


#class _ApiGlobals(set):
    #def __init__(self, *args, **kwargs):
        #set.__init__(self, *args, **kwargs)

# api_globals = set()  # holds all the public objects  # _ApiGlobals()  # holds all the public
#
#
# class _ApiCallStack(object):
#
#     def __init__(self):
#         self._depth = 0
#
#     @property
#     def is_empty(self):
#         return self._depth == 0
#
#     def inc(self):
#         self._depth += 1
#
#     def dec(self):
#         self._depth -= 1
#         if self._depth < 0:
#             self._depth = 0
#             raise RuntimeError("Internal error: API call stack tracking went below zero")
#
# _api_call_stack = _ApiCallStack()
#
#
# class ApiCallLoggingContext(object):
#
#     def __init__(self, call_stack, call_logger, call_depth, function, *args, **kwargs):
#         self.logger = None
#         self.call_stack = call_stack
#         if self.call_stack.is_empty:
#             self.logger = call_logger
#             self.depth = call_depth
#             self.function = function
#             self.args = args
#             self.kwargs = kwargs
#
#     def __enter__(self):
#         if self.call_stack.is_empty:
#             log_api_call(self.depth, self.function, *self.args, **self.kwargs)
#         self.call_stack.inc()
#
#     def __exit__(self, exception_type, exception_value, traceback):
#         self.call_stack.dec()
#         if exception_value:
#             error = IaError(self.logger)
#
#
#
#             raise error  # see intelanalytics.errors.last for details
#
#
#
# def api_context(logger, depth, function, *args, **kwargs):
#     global _api_call_stack
#     return ApiCallLoggingContext(_api_call_stack, logger, depth, function, *args, **kwargs)



# def get_api_context_decorator(logger):
#     """Provides an API decorator which will wrap functions designated as an API"""
#
#     # Note: extra whitespace lines in the code below is intentional for pretty-printing when error occurs
#     def _api(function, *args, **kwargs):
#         with api_context(logger, 4, function, *args, **kwargs):
#             try:
#                 check_api_is_loaded()
#                 return function(*args, **kwargs)
#             except:
#                 error = IaError(logger)
#
#
#
#                 raise error  # see intelanalytics.errors.last for details
#
#
#
#     def api(item):
#         if inspect.isclass(item):
#             api_globals.add(item)
#             return item
#         else:
#             try:
#                 classname = inspect.getouterframes(inspect.currentframe())[1][3]
#             except:
#                 pass
#             # must be a function, wrap with logging, error handling, etc.
#             d = decorator(_api, item)
#             d.is_api = True  # add 'is_api' attribute for meta query
#             return d
#     return api
#
#
# from intelanalytics.meta.metaprog2 import get_installation
#
#
# _client_api_installations = []
#
# from collections import namedtuple
# from intelanalytics.meta.command import CommandDefinition, Parameter, Doc, ReturnInfo
# #from intelanalytics.meta.metaprog2 import get_args_and_kwargs_from_function
#
#
# ArgDoc = namedtuple("ArgDoc", ['name', 'data_type', 'description'])
# ReturnDoc = namedtuple("ReturnDoc", ['data_type', 'description'])
#
#
# class ClientCommandDefinition(CommandDefinition):
#
#
#     def __init__(self, class_name, function, doc, *arg_and_ret_docs): # json_schema, full_name, parameters, return_info, doc=None, maturity=None, version=None, client_function=None):
#
#         # Note: this code runs during package init (before connect)
#
#         self.client_function = function
#         function.command = self  # make command def accessible from function, just like functions gen'd from server info
#
#         if not isinstance(doc, Doc):
#             raise TypeError("Internal Error: @api decorator expected a Doc for its first argument for the function %s.  Received type %s" % (function.__name__, type(doc)))
#
#         json_schema = {}  # make empty, since this command didn't come from json, and there is no need to generate it
#         full_name = self._generate_full_name(class_name, function)
#
#         params = []
#         return_info = None
#
#         if arg_and_ret_docs:
#             args, kwargs = get_args_and_kwargs_from_function(function, ignore_private_args=True)
#             num_args, num_kwargs = len(args), len(kwargs)
#             for i in xrange(0, num_args + num_kwargs):
#                 arg_name = args[i] if i < num_args else kwargs[i][0]
#                 try:
#                     arg_doc = arg_and_ret_docs[i]
#                 except IndexError:
#                     raise ValueError("@api decorator didn't received enough arguments; expected an ArgDoc for argument '%s' in function %s." % (arg_name, function.__name__))
#                 if not isinstance(arg_doc, ArgDoc):
#                     raise TypeError("@api decorator expected an ArgDoc for argument '%s' in function %s.  Received type %s" % (arg_name, function.__name__, type(arg_doc)))
#                 if arg_name != arg_doc.name:
#                     raise ValueError("@api decorator expected an ArgDoc for argument '%s' in function %s.  Received one for argument %s at the expected position" % (arg_name, function.__name__, arg_doc.name))
#                 optional, default = (False, None) if i < num_args else (True, kwargs[i][0])
#                 params.append(Parameter(name=arg_doc.name, data_type=arg_doc.data_type, use_self=False, optional=optional, default=default, doc=arg_doc.description))
#
#             num_remaining_docs = len(arg_and_ret_docs) - num_args - num_kwargs
#             if num_remaining_docs == 1:
#                 ret_doc = arg_and_ret_docs[-1]
#                 if not isinstance(ret_doc, ReturnDoc):
#                     if isinstance(ret_doc, ArgDoc):
#                         raise TypeError("@api decorator received an extra ArgDoc parameter '%s' in function %s." % (getattr(ret_doc, "name"), function.__name__))
#                     else:
#                         raise TypeError("@api decorator expected an ReturnDoc in function %s for the last argument.  Received type %s." % (function.__name__, type(ret_doc)))
#                 return_info = ReturnInfo(ret_doc.data_type, use_self=False, doc=ret_doc.description)  # todo: remove use_self from ReturnInfo
#             elif num_remaining_docs > 1:
#                 num_expected = num_args + num_kwargs
#                 raise ValueError("@api decorator received too many doc arguments.  Expected %s or %s but received %s." %(num_expected, num_expected + 1, len(arg_and_ret_docs)))
#
#         super(ClientCommandDefinition, self).__init__(json_schema, full_name, params, return_info, doc)
#
#     def _generate_full_name(self, class_name, function):
#         # reverse engineer the full_name, for command_def's sake
#         if function.__name__[0] != '_':
#             raise RuntimeError("Internal Error: @api decoration on item which does not start with an underscore: %s" % function)
#         entity_type = class_name_to_entity_type(class_name)
#         full_name = "%s/%s" % (entity_type, function.__name__[1:])  # [1:] means skip the underscore
#         return full_name
#
#
# class InitCommandDefinition(ClientCommandDefinition):
#
#     def __init__(self, class_name, function, doc, *arg_and_ret_docs): # json_schema, full_name, parameters, return_info, doc=None, maturity=None, version=None, client_function=None):
#         super(InitCommandDefinition, self).__init__(class_name, function, doc, *arg_and_ret_docs)
#         self.args_text = get_args_text_from_function(function, ignore_private_args=True)
#
#     def _generate_full_name(self, class_name, function):
#         return "%s/__init__" % class_name_to_entity_type(class_name)
#
#     def get_function_parameters_text(self):
#         return self.args_text
#
#
# def _swallowed_api(*args, **kwargs):
#     """Canned error method which replaces all client-side defined API functions"""
#     raise RuntimeError("private, swallowed method --do not call.  Install the API by calling connect()")

# @api2(Doc('one liner', 'extended'),
#       ArgDoc("name", str, description="awesome"),
#       ArgDoc("name2", "int", description="Jimbobobobo"),
#       ReturnDoc(str, 'extra comments on the return object'))
# def get_api_decorator(execution_logger):
#
#
#     # 1. create api_context executor with provided logger
#     exec_logger = execution_logger
#
#     # Note: extra whitespace lines in the code below is intentional for pretty-printing when error occurs
#     def execute_in_api_context(function, *args, **kwargs):
#         with api_context(logger, 4, function, *args, **kwargs):
#             try:
#                 check_api_is_loaded()
#                 return function(*args, **kwargs)
#             except:
#                 error = IaError(exec_logger)
#
#
#
#                 raise error  # see intelanalytics.errors.last for python details
#
#
#
#
#     # 2. create and return API decorator
#     def parameterized_api_decorator(doc, *arg_and_ret_docs):
#         """
#         Acts as a decorator for API classes and methods
#
#         But actually takes the args and closes over them with a real decorator defined herein
#         """
#
#         # closure prep
#         _doc = doc
#         _arg_and_ret_docs = arg_and_ret_docs
#
#         def api_decorator(item):
#             """
#             Decorator for API objects
#
#             For a class, it registers it with api_globals
#
#             For a method, it "swallows" it by synthesizing and storing a client-side command def object for it and then
#             returning a canned method in its place, which raises an error if actually called.  The API installation process
#             will install (or restore) the method with a public name.  Meanwhile, its metadata is available in the general
#             meta-programming data structures for the API.
#             """
#
#             if inspect.isclass(item):
#                 api_globals.add(item)
#                 return item
#
#             # for a method, we need the name of its class
#             try:
#                 # http://stackoverflow.com/questions/306130/python-decorator-makes-function-forget-that-it-belongs-to-a-class
#                 class_name = inspect.getouterframes(inspect.currentframe())[1][3]
#                 #print "classname=%s" % class_name
#             except:
#                 raise RuntimeError("Internal Error: @api decoration cannot resolve class name for item %s" % item)
#
#
#             # wrap the function with API logging and error handling
#             function = decorator(execute_in_api_context, item)
#
#             function.is_api = True  # add 'is_api' attribute for meta query
#
#             if function.__name__ == "__init__":
#                 command_def = InitCommandDefinition(class_name, function, _doc, *_arg_and_ret_docs)
#                 decorated_function = item
#             else:
#                 command_def = ClientCommandDefinition(class_name, function, _doc, *_arg_and_ret_docs)
#                 decorated_function = _swallowed_api  # return a stub which throws an error if called
#
#             _client_api_installations.append((class_name, command_def))
#             return decorated_function
#
#         return api_decorator
#
#     return parameterized_api_decorator


# def install_client_specific_apis():
#     for class_name, command_def in _client_api_installations:
#       #  function = command_def.client_function
#       #  entity_type = command_def.entity_type
#       #  full_name = command_def.full_name
#
#         # validation
#         from intelanalytics.meta.metaprog2 import get_class_from_store, install_clientside_api, InstallPath
#         cls = [c for c in api_globals if inspect.isclass(c) and c.__name__ == class_name][0]
#         cls_with_installation = get_class_from_store(InstallPath(command_def.entity_type))
#         if cls_with_installation is not cls:
#             raise RuntimeError("Internal Error: @api decoration cannot resolve class name %s for function %s" % (class_name, command_def.name))
#         installation = get_installation(cls_with_installation)
#         if command_def.entity_type != installation.install_path.full:
#             raise RuntimeError("Internal Error: @api decoration resulted in different install paths '%s' and '%s' for function %s in class %s"
#                                % (command_def.entity_type, installation.install_path, command_def.name, class_name))
#
#         install_clientside_api(cls, command_def)
#
#         # must be a function, wrap with logging, error handling, etc.
#         #d = decorator(_api, item)
#         #d.is_api = True  # add 'is_api' attribute for meta query
#         #return ''
#
# def install_clientside_api(cls, command_def):
#     from intelanalytics.meta.command import ARGS_TEXT
#     if command_def.full_name not in muted_commands:
#         function = command_def.client_function
#         function.command = command_def
#         check_loadable_class(cls, command_def)  # combine this into is_command_name_installable
#         #you're in here, trying to figure out what to clean up...  (and go back to that swallower and have fun...)
#         if function.__name__ == "__init__":
#             setattr(cls, Constants.INIT_COMMAND, command_def)  # for decorated __init__, store command in the cls itself
#         elif is_command_name_installable(cls, command_def.name):
#             setattr(cls, command_def.name, function)
#             # remove the "swallowed" stub for good housekeeping
#             try:
#                 delattr(cls, "_" + command_def.name)
#             except AttributeError:
#                 pass
#         get_installation(cls).commands.append(command_def)
#

#class _Api(object):
#     __commands_from_backend = None
#
#     @staticmethod
#     def load_api():
#         """
#         Download API information from the server, once.
#
#         After the API has been loaded, it cannot be changed or refreshed.  Also, server connection
#         information cannot change.  User must restart Python in order to change connection info.
#
#         Subsequent calls to this method invoke no action.
#         """
#         if not _Api.is_loaded():
#             install_client_specific_apis()  # first do the client-side specific processing
#
#             from intelanalytics.rest.iaserver import server
#             from intelanalytics.rest.jsonschema import get_command_def
#             #from intelanalytics.meta.metaprog import install_command_defs, delete_docstubs
#             from intelanalytics.meta.metaprog2 import install_command_defs as bonus_install_command_defs
#             from intelanalytics.meta.metaprog2 import delete_docstubs as bonus_delete_docstubs
#             logger.info("Requesting available commands from server")
#             try:
#                 response = server.get("/commands/definitions")
#             except:
#                 import sys
#                 sys.stderr.write('Unable to connect to server\n')
#                 raise IaError(logger)
#
#             commands_json_schema = response.json()
#             # ensure the assignment to __commands_from_backend is the last line in this 'if' block before the fatal try:
#             _Api.__commands_from_backend = [get_command_def(c) for c in commands_json_schema]
#             try:
#                 # install_command_defs(_Api.__commands_from_backend)
#                 # delete_docstubs()
#                 bonus_install_command_defs(_Api.__commands_from_backend)
#                 bonus_delete_docstubs()
#                 from intelanalytics import _refresh_api_namespace
#                 _refresh_api_namespace()
#             except Exception as e:
#                 _Api.__commands_from_backend = None
#                 errors._api_load = "".join(traceback.format_exception(*sys.exc_info()))
#                 raise FatalApiLoadError(e)
#
#     @staticmethod
#     def is_loaded():
#         return _Api.__commands_from_backend is not None
#
#
# load_api = _Api.load_api  # create alias for export
#
#
# class FatalApiLoadError(RuntimeError):
#     def __init__(self, e):
#         self.details = str(e)
#         RuntimeError.__init__(self, """
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Fatal error: processing the loaded API information failed and has left the
# client in a state of unknown compatibility with the server.
#
# Please contact support.
#
# Details:%s
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# """ % self.details)
#
#
# class ApiAlreadyLoadedError(RuntimeError):
#     def __init__(self):
#         RuntimeError.__init__(self, "Invalid operation because the client is already connected to "
#                                     "the intelanalytics server.  Restarting Python is required "
#                                     "if you need to perform this operation")
#
#
# class ApiNotYetLoadedError(RuntimeError):
#     def __init__(self):
#         RuntimeError.__init__(self, "Please connect to the server by calling 'connect()' before proceeding")
#
#
#
# def check_api_is_loaded():
#     if not _Api.is_loaded():
#         raise ApiNotYetLoadedError()


# decorator for methods that are invalid after api load, like changing server config
# def _error_if_api_already_loaded(function, *args, **kwargs):
#     if _Api.is_loaded():
#         raise ApiAlreadyLoadedError()
#     return function(*args, **kwargs)
#
#
# def error_if_api_already_loaded(function):
#     return decorator(_error_if_api_already_loaded, function)


from intelanalytics.meta.metaprog2 import get_installation

# methods to dump the names in the API (driven by QA coverage)
def get_api_names(obj, prefix=''):
    found = []
    names = dir(obj)
    for name in names:
        if not name.startswith("_"):
            a = getattr(obj, name)
            suffix = _get_inheritance_suffix(obj, name)
            if isinstance(a, property):
                installation = get_installation(obj, None)
                if installation:
                    intermediate_class = installation.get_intermediate_class(name)
                    if intermediate_class:
                        found.extend(get_api_names(intermediate_class, prefix + name + "."))
                a = a.fget
            if hasattr(a, "_is_api"):
                found.append(prefix + name + suffix)
                if inspect.isclass(a):
                    found.extend(get_api_names(a, prefix + name + "."))
    return found


def walk_api(cls, cls_function, attr_function, include_init=False):
    """
    Walks the API and executes the cls_function at every class and the attr_function at every attr

    Skip all private methods.  If __init__ should be included (special case), set include_init=True

    Either *_function arg can be None.  For example if you're only interesting in walking functions,
    set cls_function=None.
    """
    from intelanalytics.meta.metaprog2 import get_installation
    cls_func = cls_function
    attr_func = attr_function
    inc_init = include_init

    def walker(obj):
        for name in dir(obj):
            if not name.startswith("_") or (inc_init and name == "__init__"):
                a = getattr(obj, name)
                if isinstance(a, property):
                    installation = get_installation(obj, None)
                    if installation:
                        intermediate_class = installation.intermediates.get(name, None)
                        if intermediate_class:
                            walker(intermediate_class)
                    a = a.fget
                elif inspect.isclass(a):
                    if hasattr(a, "_is_api") and cls_func:
                        cls_func(a)
                    walker(a)
                elif hasattr(a, "_is_api"):
                    if attr_func:
                        attr_func(obj, a)
    walker(cls)


def _get_inheritance_suffix(obj, member):
    if inspect.isclass(obj):
        if member in obj.__dict__:
            return ''
        heritage = inspect.getmro(obj)[1:]
        for base_class in heritage:
            if member in base_class.__dict__:
                return "   (inherited from %s)" % base_class.__name__
    return ''
