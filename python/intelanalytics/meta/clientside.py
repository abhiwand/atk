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
Decoration and installation for the API objects defined in the python core code
"""

import inspect

from intelanalytics.core.api import api_globals

from intelanalytics.meta.classnames import class_name_to_entity_type
from intelanalytics.meta.command import CommandDefinition, Parameter, ReturnInfo
from intelanalytics.meta.context import get_api_context_decorator
from intelanalytics.meta.reflect import get_args_spec_from_function, get_args_text_from_function
from intelanalytics.meta.genspa import gen_spa


client_commands = []  # list of tuples (class_name, command_def) defined in the python client code (not from server)


class ArgDoc(object):
    def __init__(self, name, data_type, description):
        self.name = name
        self.data_type = data_type
        self. description = description


class ReturnDoc(object):
    def __init__(self, data_type, description):
        self.data_type = data_type
        self.description = description


class ClientCommandDefinition(CommandDefinition):
    """CommandDefinition for functions marked as @api in the core python code"""

    def __init__(self, class_name, member, is_property):
        # Note: this code runs during package init (before connect)

        self.client_member = member
        self.parent_name = class_name

        function = member.fget if is_property else member
        function.command = self  # make command def accessible from function, just like functions gen'd from server info

        json_schema = {}  # make empty, since this command didn't come from json, and there is no need to generate it
        full_name = self._generate_full_name(class_name, function.__name__)

        params = []
        return_info = None

        args, kwargs, varargs, varkwargs = get_args_spec_from_function(function, ignore_private_args=True)
        num_args = len(args) + len(kwargs) + (1 if varargs else 0) + (1 if varkwargs else 0)

        if hasattr(function, "arg_docs"):
            arg_docs = function.arg_docs
            num_arg_docs = len(arg_docs)
            if num_arg_docs > num_args:   # only check for greater than, the code after will give a better exception message for less than case
                raise ValueError("function received %d @arg decorators, expected %d for function %s." % (num_arg_docs, num_args, function.__name__))

            def _get_arg_doc(name):
                try:
                    arg_doc = filter(lambda  d: d.name == name, arg_docs)[0]
                except IndexError:
                    raise ValueError("Function missing @arg decorator for argument '%s' in function %s" % (name, function.__name__))
                if not isinstance(arg_doc, ArgDoc):
                    raise TypeError("InternalError - @api decorator expected an ArgDoc for argument '%s' in function %s.  Received type %s" % (name, function.__name__, type(arg_doc)))
                return arg_doc
        else:
            def _get_arg_doc(name):
                return ArgDoc(name, '', '')

        for arg_name in args:
            arg_doc = _get_arg_doc(arg_name)
            params.append(Parameter(name=arg_doc.name, data_type=arg_doc.data_type, use_self=False, optional=False, default=None, doc=arg_doc.description))

        for arg_name, default in kwargs:
            arg_doc = _get_arg_doc(arg_name)
            params.append(Parameter(name=arg_doc.name, data_type=arg_doc.data_type, use_self=False, optional=True, default=default, doc=arg_doc.description))

        for arg_name in [varargs, varkwargs]:
            if arg_name:
                arg_doc = _get_arg_doc(arg_name)
                params.append(Parameter(name=arg_doc.name, data_type=arg_doc.data_type, use_self=False, optional=True, default=None, doc=arg_doc.description))

        if hasattr(function, "return_doc"):
            return_doc = function.return_doc
            if not isinstance(return_doc, ReturnDoc):
                raise TypeError("InternalError - @returns decorator expected an ReturnDoc in function %s.  Received type %s." % (function.__name__, type(return_doc)))
            return_info = ReturnInfo(return_doc.data_type, use_self=False, doc=return_doc.description)  # todo: remove use_self from ReturnInfo

        maturity = function.maturity if hasattr(function, "maturity") else None

        super(ClientCommandDefinition, self).__init__(json_schema, full_name, params, return_info, is_property, function.__doc__, maturity=maturity)

        spa_doc = gen_spa(self)
        function.__doc__ = spa_doc

    @staticmethod
    def _generate_full_name(class_name, member_name):
        entity_type = class_name_to_entity_type(class_name)
        full_name = "%s/%s" % (entity_type, member_name)
        return full_name


class InitClientCommandDefinition(ClientCommandDefinition):
    """CommandDefinition for __init__ functions marked as @api in the core python code"""

    def __init__(self, class_name, function):
        super(InitClientCommandDefinition, self).__init__(class_name, function, False)
        self.args_text = get_args_text_from_function(function, ignore_private_args=True)

    def get_function_args_text(self):
        return self.args_text


def alpha(item):
    item.maturity = 'alpha'
    return item


def beta(item):
    item.maturity = 'beta'
    return item


def deprecated(item):
    item.maturity = 'deprecated'
    return item


def arg(name, data_type, description):
    """Decorator to describe a method argument"""
    def add_arg_doc(item):
        if not hasattr(item, 'arg_docs'):
            item.arg_docs = []
        item.arg_docs.append(ArgDoc(name, data_type, description))
        return item
    return add_arg_doc


def returns(data_type, description):
    """Decorator to describe what a method returns"""
    def add_return_doc(item):
        if hasattr(item, 'return_doc'):
            raise RuntimeError("More than one @returns decorator attached to item '%s'" % item.__name__)
        else:
            item.return_doc = ReturnDoc(data_type, description)
        return item
    return add_return_doc


def is_api(item):
    return hasattr(item, "_is_api") and getattr(item, "_is_api")

def mark_item_as_api(item):
    item._is_api = True


def decorate_api_class(item):
    mark_item_as_api(item)
    api_globals.add(item)
    return item


def get_api_decorator(logger, parent_class_name=None):
    """gets an @api decorator for the given logger"""

    execution_logger = logger

    def api_decorator(item):
        """
        Decorator for API objects

        For a class, it registers it with api_globals

        For a method, it "swallows" it by synthesizing and storing a client-side command def object for it and then
        returning a canned method in its place, which raises an error if actually called.  The API installation process
        will install (or restore) the method with a public name.  Meanwhile, its metadata is available in the general
        meta-programming data structures for the API.

        Note: this @api decorator must be the first decorator when combined with other decorators from this package.
        The python @property decorator would come before this one.  Example:

        @api
        @beta
        @arg('n', int, 'number of bananas')
        def feed_apes(n):
            '''
            One line summary to say feed the apes.

            Extended summary to describe the side-effects
            of feeding of the apes.
            '''
       """

        if inspect.isclass(item):
            return decorate_api_class(item)

        is_property = isinstance(item, property)
        attr = item.fget if is_property else item

        mark_item_as_api(attr)

        # for a method, we need the name of its class
        if parent_class_name:
            class_name = parent_class_name
        else:
            try:
                # http://stackoverflow.com/questions/306130/python-decorator-makes-function-forget-that-it-belongs-to-a-class
                outerframes = inspect.getouterframes(inspect.currentframe())
                call_depth_to_class = 1
                class_name = outerframes[call_depth_to_class][3]
                #print "classname=%s" % class_name
            except:
                raise RuntimeError("Internal Error: @api decoration cannot resolve class name for item %s" % item)

        _patch_member_name(class_name, attr)

        # wrap the function with API logging and error handling
        function = get_api_context_decorator(execution_logger)(attr)

        if function.__name__ == "__init__":
            command_def = InitClientCommandDefinition(class_name, function)
        else:
            member = property(fget=function, fset=item.fset) if is_property else function
            command_def = ClientCommandDefinition(class_name, member, is_property)

        client_commands.append((class_name, command_def))

        return clientside_api_stub

    return api_decorator


def _patch_member_name(class_name, member):
    """Patch up the name of the member.  ie. remove leading underscores"""
    prefix = "__"
    name = member.__name__
    if name[:len(prefix)] != prefix:
        raise RuntimeError("@api applied to incorrectly named member '%s' in object '%s'" % (name, class_name))
    if name != '__init__':
        member.__name__ = name[len(prefix):]


def clientside_api_stub():
    """Filler method stub for the decorator to return, should never be called"""
    raise RuntimeError("Illegal function call.  This method should have been cleaned up during command installation.")


def clear_clientside_api_stubs(cls):
    """Deletes all the clientside_api_stub attributes from the cls"""
    victims = [k for k, v in cls.__dict__.items() if v is clientside_api_stub]
    for v in victims:
        delattr(cls, v)

