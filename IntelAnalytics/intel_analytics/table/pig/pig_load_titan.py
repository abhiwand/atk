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
import sys

#Coverage.py will attempt to import every python module to generate coverage statistics.
#Since Pig is only available to Jython than this will cause the coverage tool to throw errors thus breaking the build.
#This try/except block will allow us to run coverage on the Jython files.
try:
    from org.apache.pig.scripting import Pig
except:
    print("Pig is either not installed or not executing through Jython. Pig is required for this module.")
from intel_analytics.table.pig.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from intel_analytics.config import global_config as config

def main(argv):
    parser = ArgumentParser(description='loads Titan graph from frame')
    parser.add_argument('-t', '--table', dest='input_table', help='name of the input HBase table that GraphBuilder (GB) will create the graph from', required=True)
    parser.add_argument('-d', '--directed', dest='is_directed', help='supplied if edges should be directed, otherwise they are undirected', required=False)
    parser.add_argument('-v', '--vertex', dest='vertex_list', help='vertex list', required=True)
    parser.add_argument('-e', '--edge', dest='edge_list', help='edge list', required=True)
    parser.add_argument('-c', '--config', dest='config', help='path to the XML configuration file to be used by GB for bulk loading to Titan', required=True)
    parser.add_argument('-o', '--overwrite', dest='overwrite', help='graph will be overwritten', required=False)
    parser.add_argument('-a', '--append', dest='append', help='graph will be appended to', required=False)
    parser.add_argument('-f', '--flatten', dest='flatten', help='flatten lists', required=False)

    cmd_line_args = parser.parse_args()

    other_args = ""
    if cmd_line_args.overwrite:
        other_args += " -O "
    if cmd_line_args.append:
        other_args += " -a "
    if cmd_line_args.flatten:
        other_args += " -F "

    edge_rule = ('-d ' if cmd_line_args.is_directed else '-e ') + cmd_line_args.edge_list
    vertex_rule = cmd_line_args.vertex_list

    pig_statements = ["IMPORT '" + config['graph_builder_pig_udf'] + "';",
                      "LOAD_TITAN('%s', '%s', '%s', '%s', '%s');" %
                      (cmd_line_args.input_table, vertex_rule, edge_rule, cmd_line_args.config, other_args)]

    pig_script = "\n".join(pig_statements)
    compiled = Pig.compile(pig_script)
    status = compiled.bind().runSingle()
    return 0 if status.isSuccessful() else 1

if __name__ == "__main__":
    try:
        rc = main(sys.argv)
        sys.exit(rc)
    except Exception, e:
        print e
        sys.exit(1)
