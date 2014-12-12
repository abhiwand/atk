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
Logging - simple helpers for now
"""
import logging
import sys

# Constants
API_LOGGER_NAME = 'IA Python API'
HTTP_LOGGER_NAME = 'intelanalytics.rest.connection'
LINE_FORMAT = '%(asctime)s|%(name)s|%(levelname)-5s|%(message)s'
API_LINE_FORMAT = '|api|  %(message)s'

# add a null handler to root logger to avoid handler warning messages
class NullHandler(logging.Handler):
    name = "NullHandler"

    def emit(self, record):
        pass

# this line avoids the 'no handler' warning msg when no logging is set at all
_null_handler = NullHandler()
_null_handler.name = ''  # add name explicitly for python 2.6
logging.getLogger('').addHandler(_null_handler)


class Loggers(object):
    """
    Collection of loggers to stderr, wrapped for simplicity
    """
    # todo - WIP, this will get more sophisticated!

    # map first character of level to actual level setting, for convenience
    _level_map = {'c': logging.CRITICAL,
                  'f': logging.FATAL,
                  'e': logging.ERROR,
                  'w': logging.WARN,
                  'i': logging.INFO,
                  'd': logging.DEBUG,
                  'n': logging.NOTSET}

    def __init__(self):
        self._user_logger_names = []

    def __repr__(self):
        header = ["{0:<8}  {1:<50}  {2:<14}".format("Level", "Logger", "# of Handlers"),
                  "{0:<8}  {1:<50}  {2:<14}".format("-"*8, "-"*50, "-"*14)]
        entries = []
        for name in self._user_logger_names:
            entries.append(self._get_repr_line(name, None))
        return "\n".join(header + entries)

    @staticmethod
    def _get_repr_line(name, alias):
        logger = logging.getLogger(name)
        if alias:
            name += " (%s)" % alias
        return "{0:<8}  {1:<50}  {2:<14}".format(logging.getLevelName(logger.level),
                                                 name,
                                                 len(logger.handlers))

    def set_http(self, level=logging.DEBUG, output=None):
        """
        Sets the level of logging for http traffic

        Parameters
        ----------
        level : int, str or logging.*, optional
            The level to which the logger will be set.  May be 0,10,20,30,40,50
            or "DEBUG", "INFO", etc.  (only first letter is requirecd)
            Setting to None disables the logging to stderr
            See `https://docs.python.org/2/library/logging.html`
            If not specified, DEBUG is used
            To turn OFF the logger, set level to 0 or None
        output: file or str, or list of such, optional
            The file object or name of the file to log to.  If empty, then stderr is used

        Examples
        --------
        >>> loggers.set_http()       # Enables http logging to stderr
        >>> loggers.set_http(None)   # Disables http logging
        """
        self.set(level, HTTP_LOGGER_NAME, output)

    def set_api(self, level=logging.INFO, output=sys.stdout, line_format=API_LINE_FORMAT):
        """
        Sets the level of logging for py api calls (command tracing)

        Parameters
        ----------
        level : int, str or logging.*, optional
            The level to which the logger will be set.  May be 0,10,20,30,40,50
            or "DEBUG", "INFO", etc.  (only first letter is requirecd)
            Setting to None disables the logging to stderr
            See `https://docs.python.org/2/library/logging.html`
            If not specified, DEBUG is used
            To turn OFF the logger, set level to 0 or None
        output: file or str, or list of such, optional
            The file object or name of the file to log to.  If empty, then stderr is used
        line_format: str, optional
            By default, the api logger has a simple format suitable for interactive use
            To get the standard logger formatting with timestamps, etc., set line_format=None

        Examples
        --------
        >>> loggers.set_api()       # Enables api logging to stdout
        >>> loggers.set_api(None)   # Disables api logging
        """
        self.set(level, API_LOGGER_NAME, output, line_format)

    def set(self, level=logging.DEBUG, logger_name='', output=None, line_format=None):
        """
        Sets the level and adds handlers to the given logger

        Parameters
        ----------
        level : int, str or logging.*, optional
            The level to which the logger will be set.  May be 0,10,20,30,40,50
            or "DEBUG", "INFO", etc.  (only first letter is requirecd)
            Setting to None disables the logging to stderr
            See `https://docs.python.org/2/library/logging.html`
            If not specified, DEBUG is used
            To turn OFF the logger, set level to 0 or None
        logger_name: str, optional
            The name of the logger.  If empty string, then the intelanalytics root logger is set
        output: file or str, or list of such, optional
            The file object or name of the file to log to.  If empty, then stderr is used

        Examples
        --------
        # to enable INFO level logging to file 'log.txt' and no printing to stderr:
        >>> loggers.set('INFO', 'intelanalytics.rest.frame','log.txt', False)
        """
        logger_name = logger_name if logger_name != 'root' else ''
        if not level:
            return self._turn_logger_off(logger_name)

        line_format = line_format if line_format is not None else LINE_FORMAT
        logger = logging.getLogger(logger_name)
        if not output:
            output = sys.stderr
        if isinstance(output, basestring):
            handler = logging.FileHandler(output)
        elif isinstance(output, list) or isinstance(output, tuple):
            logger = None
            for o in output:
                logger = self.set(level, logger_name, o, line_format)
            return logger
        else:
            try:
                handler = logging.StreamHandler(output)
            except:
                raise ValueError("Bad output argument %s.  Expected stream or file name." % output)

        try:
            handler_name = output.name
        except:
            handler_name = str(output)

        if isinstance(level, basestring):
            c = str(level)[0].lower()  # only require first letter
            level = self._level_map[c]
        logger.setLevel(level)

        self._add_handler_to_logger(logger, handler, handler_name, line_format)

        # store logger name
        if logger_name not in self._user_logger_names:
            self._user_logger_names.append(logger_name)
        return logger

    @staticmethod
    def _logger_has_handler(logger, handler_name):
        return logger.handlers and any([h.name for h in logger.handlers if h.name == handler_name])

    @staticmethod
    def _add_handler_to_logger(logger, handler, handler_name, line_format):
        handler.setLevel(logging.DEBUG)
        handler.name = handler_name
        formatter = logging.Formatter(line_format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def _turn_logger_off(self, logger_name):
        logger = logging.getLogger(logger_name)
        logger.level = logging.CRITICAL
        for h in logger.handlers:
            logger.removeHandler(h)
        try:
            self._user_logger_names.remove(logger_name)
        except ValueError:
            pass
        return logger

loggers = Loggers()


# API logger
_api_logger = logging.getLogger(API_LOGGER_NAME)


def log_api_call(function, *args, **kwargs):
    """Logs the call of the given function with the API logger"""
    _api_logger.info(ApiLogFormat.format_call(function, *args, **kwargs))


class ApiLogFormat(object):
    """Formats api calls for logging"""

    @staticmethod
    def format_call(function, *args, **kwargs):
        try:
            param_names = function.func_code.co_varnames[0:function.func_code.co_argcount]
            named_args = zip(param_names, args)
            self = None
            try:
                if param_names[0] == "self":
                    self = args[0]
                    named_args = named_args[1:]
            except:
                self = None
            named_args.extend(kwargs.items())
            formatted_args = '' if not named_args else "(%s)" % (", ".join([ApiLogFormat.format_named_arg(k,v) for k, v in named_args]))

            return "%s%s%s" % (ApiLogFormat.format_self(self), ApiLogFormat.format_function(function), formatted_args)
        except Exception as e:
            return str(e)

    @staticmethod
    def format_named_arg(name, value):
        return "%s=%s" % (name, ApiLogFormat.format_value(value))

    @staticmethod
    def format_function(f):
        return f.__name__

    @staticmethod
    def format_self(v):
        return (ApiLogFormat.format_value(v) + ".") if v is not None else ''

    @staticmethod
    def format_value(v):
        if hasattr(v, "_id"):
            return "<%s:%s>" % (type(v).__name__, hex(id(v))[2:])
        return repr(v)

