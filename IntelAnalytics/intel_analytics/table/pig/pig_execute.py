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
import os
import sys

#Coverage.py will attempt to import every python module to generate coverage statistics.
#Since Pig is only available to Jython than this will cause the coverage tool to throw errors thus breaking the build.
#This try/except block will allow us to run coverage on the Jython files.
from intel_analytics.config import global_config as config
from intel_analytics.table.pig.pig_helpers import report_job_status

try:
    from org.apache.pig.scripting import Pig
except:
    print("Pig is either not installed or not executing through Jython. Pig is required for this module.")
from intel_analytics.table.pig.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7


def main(argv):
    parser = ArgumentParser(description='Excute pig script')
    parser.add_argument('-s', '--script', dest='script', help='the script for pig execution', required=True)
    cmd_line_args = parser.parse_args()
    input_script = cmd_line_args.script
    statements = []
    statements.append("REGISTER %s;" % (config['feat_eng_jar']))
    statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script
    statements.append("SET default_parallel %s;" % (config['pig_parallelism_factor']))
    statements.append(input_script)
    pig_script = "\n".join(statements)
    compiled = Pig.compile(pig_script)
    status = compiled.bind().runSingle()

    report_job_status(status)
    return 0 if status.isSuccessful() else 1


if __name__ == "__main__":
    try:
        rc = main(sys.argv)
        sys.exit(rc)
    except Exception, e:
        print e
        sys.exit(1)
