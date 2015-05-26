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
Admin commands, not part of public API
"""

from intelanalytics.rest.command import execute_command


def _explicit_garbage_collection(age_to_delete_data = None, age_to_delete_meta_data = None):
    """
    Execute garbage collection out of cycle age ranges specified using the typesafe config duration format.
    :param age_to_delete_data: Minimum age for data deletion. Defaults to server config.
    :param age_to_delete_meta_data: Minimum age for meta data deletion. Defaults to server config.
    """
    execute_command("_admin:/_explicit_garbage_collection", None,
                    age_to_delete_data=age_to_delete_data,
                    age_to_delete_meta_data=age_to_delete_meta_data)

