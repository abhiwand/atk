##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
#TODO need more functionality like logging to file, setting the log levels, etc.
import logging
from logging.handlers import RotatingFileHandler
import os
import datetime

levels = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}
try:
    from intel_analytics.config import global_config as conf

    logger_level = levels[conf['py_logger_level'].upper()]
except:
    logger_level = logging.ERROR

logging.basicConfig(level=logger_level)
stdout_logger = logging.getLogger(__name__)


def get_file_name_from_datetime(now):
    return ('%s_%s') % ('python', now.strftime("%Y%m%d-%H%M%S%f"))

if not os.getenv('IN_UNIT_TESTS'):
    hdlr = RotatingFileHandler(filename=os.path.join(conf['logs_folder'],
                                                     get_file_name_from_datetime(datetime.datetime.now())),
                               maxBytes=int(conf['python_log_max_bytes']),
                               backupCount=int(conf['python_log_backup_count']))
    stdout_logger.addHandler(hdlr)

stdout_logger.setLevel(logger_level)
stdout_logger.propagate = 0




