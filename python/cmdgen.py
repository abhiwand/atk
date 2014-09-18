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
Creates the auto*.py files
"""

import json
import subprocess
import glob
import os
import warnings

dirname = os.path.dirname
here = dirname(__file__)

full_path_to_core = os.path.join(here, r'intelanalytics/core')

# Must delete any existing autos BEFORE importing BigFrame, BigGraph...
for existing_auto in glob.glob("%s/auto*.py*" % full_path_to_core):
    print "Deleting existing %s" % existing_auto
    os.remove(existing_auto)


# import the loadable's and expect NOT inheriting warnings
with warnings.catch_warnings(record=True) as expected_warnings:
    warnings.simplefilter("always")

    from intelanalytics.core.frame import BigFrame
    from intelanalytics.core.graph import BigGraph

    assert len(expected_warnings) == 2
    for w in expected_warnings:
        assert issubclass(w.category, RuntimeWarning)
        assert "NOT inheriting commands" in str(w.message)


from intelanalytics.core.metaprog import load_loadable, get_auto_module_text
from intelanalytics.rest.jsonschema import get_command_def


def full_paths(d):
    return dict([(cls, os.path.join(full_path_to_core, filename)) for cls, filename in d.items()])

autos = full_paths({BigFrame: 'autoframe.py', BigGraph: 'autograph.py'})

cmd = os.path.join(here, r'../bin/engine-spark.sh')
print "Calling engine-spark to dump the command json file: %s" % cmd
subprocess.call(cmd)

# Get the command definitions, which should have been dumped by the engine-spark's CommandDumper
print "Opening dump file and pulling in the command defintions"
with open("../target/command_dump.json", 'r') as f:
    commands = [get_command_def(json_schema) for json_schema in json.load(f)['commands']]


def write_auto_file(loadable_class, filename):
    print "Writing file %s for %s" % (filename, loadable_class)
    load_loadable(loadable_class, commands, None)  # None for execute_command function, since we're writing it in text
    # Now we can also use the _created_classes global variable from metaprog, which
    # is conveniently holding all the dynamically generated member classes
    text = get_auto_module_text(loadable_class)
    with open(filename, 'w') as f:
        f.write(text)

for loadable_class, path in autos.items():
    write_auto_file(loadable_class, path)
