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
"""
Running Pig jython scripts
"""
import os
from intel_analytics.config import global_config as config
from intel_analytics.logger import stdout_logger as logger

def get_pig_args(script_name, additional_pig_args=[]):
    """
    Get arguments for running pig, optionally providing any additional args wanted

    For example, given the parameter 'pig_load_titan.py' this method returns something
    like [ 'pig', '-4', 'path/to/pig_log4j.properties', 'path/to/pig_load_titan.py' ]
    """
    args=['pig']
    if is_local_run():
        args += ['-x', 'local']

    pig_log4j_path = os.path.join(config['conf_folder'], 'pig_log4j.properties')
    logger.debug('Using %s '% pig_log4j_path)

    args += ['-4', pig_log4j_path]
    args += additional_pig_args
    args += [os.path.join(config['pig_py_scripts'], script_name)]
    return args

def get_pig_args_with_gb(script_name, additional_pig_args=[]):
    """
    Get arguments for running pig including the additional_pig_args needed for
    running Graph Builder Macros and UDF's
    """
    more_args = additional_pig_args[:]
    more_args += ['-param', 'GB_JAR=' + config['graph_builder_jar'] ]
    return get_pig_args(script_name, more_args)

def is_local_run():
    """
    Running pig in local mode is faster for developer testing
    """
    try:
        #for quick testing
        return config['local_run'].lower().strip() == 'true'
    except:
        return False