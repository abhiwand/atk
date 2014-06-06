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
#############################################################################
import sys,traceback

from intelanalytics.core.loggers import *

# Values to be initialized:
# Set stack_flag to print the stack trace. If set to false, only the error message is printed.
# file_name is the name of the logger file. We could pick this up from a config file to ensure we have the same file everywhere
# std_err flag is to be enabled if we want to log output to the std err console. The logging level here is DEBUG.

def get_stacktrace(exc):
    stack_flag=True
    file_name = 'log.out'
    stderr_flag = False

    exc_type,exc_value,exc_traceback = exc

    # Logging the stacktrace at INFO level
    # Syntax: set(level, logger_name, file_name, stderr_flag)
    logger = loggers.set('INFO','loggername',file_name,stderr_flag)
    if stack_flag == True:
        # **** Printing the stack trace: ****
        logger.info(repr(traceback.extract_tb(exc_traceback)))
    else:
        # **** Printing the error message is: ****
        logger.info(traceback.format_exc(limit=0))