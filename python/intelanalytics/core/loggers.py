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


class Loggers(object):
    """
    Collection of loggers to stderr, wrapped for simplicity
    """
    # todo - WIP, this will get more sophisticated!

    # copy levels from logging for convenience to user
    CRITICAL = logging.CRITICAL
    FATAL = logging.FATAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    WARN = logging.WARN
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET

    def __init__(self):
        self._logger_names = []

    def __repr__(self):
        if not self._logger_names:
            return "No loggers set.  (Use loggers.set(level, logger_name))"
        return "\n".join([self._logger_repr(logging.getLogger(name))
                          for name in self._logger_names])

    @staticmethod
    def _logger_repr(logger):
        return "{0:9} '{1}'".format(logging.getLevelName(logger.level), logger.name)

    def set(self, level=logging.DEBUG, logger_name=''):
        logger = logging.getLogger(logger_name)
        if level is None:
            # turn logger OFF
            logger.level = logging.CRITICAL
            for h in logger.handlers:
                if h.name == 'stderr':
                    logger.removeHandler(h)
            try:
                self._logger_names.remove(logger_name)
            except ValueError:
                pass
        else:
            logger.setLevel(level)
            if not logger.handlers:
                if not [h.name for h in logger.handlers if h.name == 'stderr']:
                    h = logging.StreamHandler()
                    h.name = 'stderr'
                    h.setLevel(logging.DEBUG)
                    logger.addHandler(h)
            if logger_name not in self._logger_names:
                self._logger_names.append(logger_name)


loggers = Loggers()
