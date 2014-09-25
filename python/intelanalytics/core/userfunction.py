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
import re
import sys
from decorator import decorator

def _has_python_user_function_arg(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except:
        exc_info = sys.exc_info()
        e = exc_info[1]
        message = str(e)
        lines = message.split("\n")

        eligibal_lines = []

        # match the server side stack trace from running python user function and remove it
        regex = re.compile(".*java:[0-9]+.*|.*scala:[0-9]+.*|Driver stacktrace.*")
        for line in lines:
            if regex.search(line) is None:
                eligibal_lines.append(line)

        message = "\n".join(eligibal_lines)
        e.args = (message,)
        raise e

def has_python_user_function_arg(function):
    return decorator(_has_python_user_function_arg, function)

