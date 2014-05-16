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


# add a null handler to root logger to avoid handler warning messages
class NullHandler(logging.Handler):
    def emit(self, record):
        pass

# this line avoids the 'no handler' warning msg when no logging is set at all
logging.getLogger('').addHandler(NullHandler())


class Loggers(object):
    """
    Collection of loggers to stderr, wrapped for simplicity
    """
    # todo - WIP, this will get more sophisticated!

    _line_format = '%(asctime)s|%(name)s|%(levelname)-5s|%(message)s'

    # table of aliased loggers for easy reference in REPL
    _aliased_loggers_map = {
        'rest_connection': 'intelanalytics.rest.connection',
        }

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
        self._create_alias_set_functions()

    def __repr__(self):
        header = ["{0:<40} {1:<10} {2:<14}".format("Logger", "Level", "# of Handlers"),
                  "{0:<40} {1:<10} {2:<14}".format("-"*39, "-"*9, "-"*13)]
        entries = []
        names = self._aliased_loggers_map.values() + self._user_logger_names
        for name in names:
            logger = logging.getLogger(name)

            entries.append("{0:<40} {1:<10} {2:7}".
                           format(logger.name,
                                  logging.getLevelName(logger.level),
                                  len(logger.handlers)))
        return "\n".join(header + entries)

    def _create_alias_set_functions(self):
        """
        Creates set methods for aliased loggers and puts them in self.__dict__
        """
        for alias, name in self._aliased_loggers_map.items():
            def alias_set(level=logging.DEBUG):
                self.set(level, name)
            try:
                doc = Loggers.set.__doc__
                alias_set.__doc__ = doc[:doc.index("logger_name")] + """
        Examples
        --------
        >>> loggers.%s_set('debug')""" % alias
            except:
                pass
            self.__dict__[alias + "_set"] = alias_set

    def set(self, level=logging.DEBUG, logger_name=''):
        """
        Set the level of the logger going to stderr

        Parameters
        ----------
        level : int, str or logging.*, optional
            The level to which the logger will be set.  May be 0,10,20,30,40,50
            or "DEBUG", "INFO", etc.  (only first letter is requirecd)
            Setting to None disables the logging to stderr
            See `https://docs.python.org/2/library/logging.html`
        logger_name: str, optional
            The name of the logger.  If empty string, then the intelanalytics
            root logger is set

        Examples
        --------
        >>> loggers.set('debug', 'intelanalytics.rest.connection')
        """
        logger_name = logger_name if logger_name != 'root' else ''
        logger = logging.getLogger(logger_name)
        if level is None or level == '':
            # turn logger OFF
            logger.level = logging.CRITICAL
            for h in logger.handlers:
                if h.name == 'stderr':
                    logger.removeHandler(h)
            try:
                self._user_logger_names.remove(logger_name)
            except ValueError:
                pass
        else:
            if isinstance(level, basestring):
                c = str(level)[0].lower()  # only require first letter
                level = self._level_map[c]
            logger.setLevel(level)
            if not logger.handlers or not [h.name for h in logger.handlers if h.name == 'stderr']:
                # logger does not have a handler to stderr, so we'll add it
                h = logging.StreamHandler()
                h.name = 'stderr'
                h.setLevel(logging.DEBUG)
                formatter = logging.Formatter(self._line_format)
                h.setFormatter(formatter)
                logger.addHandler(h)
            if logger_name not in self._user_logger_names + self._aliased_loggers_map.values():
                self._user_logger_names.append(logger_name)


loggers = Loggers()
