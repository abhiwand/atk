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

import sys
import traceback
import warnings


class Errors(object):
    """
    Settings and methods for Python API layer error handling
    """
    def __init__(self):
        self._show_details = False
        self._last_exc_info = None  # the last captured API layer exception

    def __repr__(self):
        return "show_details = %s\nlast = %s" % (self._show_details, self.last)

    _help_msg = """(For full stack trace of this error, use: errors.last
 To always show full details, set errors.show_details = True)
"""

    @property
    def show_details(self):
        """Boolean which determines if the full exception traceback is included in the exception messaging"""
        return self._show_details

    @show_details.setter
    def show_details(self, value):
        self._show_details = value

    @property
    def last(self):
        """String containing the details (traceback) of the last captured exception"""
        last_exception = self._get_last()
        return ''.join(last_exception) if last_exception else None

    def _get_last(self):
        """Returns list of formatted strings of the details (traceback) of the last captured exception"""
        if self._last_exc_info:
            (exc_type, exc_value, exc_tb) = self._last_exc_info
            return traceback.format_exception(exc_type, exc_value, exc_tb)
        else:
            return None

# singleton
errors = Errors()


class IaError(Exception):
    """
    Internal Error Factory for the API layer to report or remove error stack trace.

    Use with raise

    Examples
    --------
    >>> try:
    ...    x = 4 / 0  # some work...
    ... except:
    ...    raise IaError()
    """
    def __new__(cls, logger=None):
        exc_info = sys.exc_info()
        errors._last_exc_info = exc_info
        try:
            cls.log_error(logger)
        except:
            warnings.warn("Unable to log exc_info", RuntimeWarning)

        if errors.show_details:
            # to show the stack, we just re-raise the last exception as is
            raise
        else:
            # to hide the stack, we return the exception info w/o trace
            #sys.stderr.write(errors._help_msg)
            #sys.stderr.flush()
            return exc_info[1], None, None

    @classmethod
    def log_error(cls, logger=None):
        if logger:
            message = traceback.format_exc(limit=None)
            logger.error(message)