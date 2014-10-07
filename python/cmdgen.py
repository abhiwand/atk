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
Creates the docstubs.py API documentation file
"""

import json
import subprocess
import glob
import os
import warnings
import inspect

dirname = os.path.dirname
here = dirname(__file__)

full_path_to_core = os.path.join(here, r'intelanalytics/core')

DOCSTUBS_FILE = 'docstubs.py'

file_name = os.path.join(full_path_to_core, DOCSTUBS_FILE)

# Must delete any existing docstub.py files BEFORE importing ia
for existing_doc_file in glob.glob("%s*" % file_name):  # * on the end to get the .pyc as well
    print "Deleting existing %s" % existing_doc_file
    os.remove(existing_doc_file)


# import the loadable's and expect NOT inheriting warnings
with warnings.catch_warnings(record=True) as expected_warnings:
    warnings.simplefilter("always")

    import intelanalytics as ia

    # TODO - turn this check back on...
    # for w in expected_warnings:
    #     assert issubclass(w.category, RuntimeWarning)
    #     assert "NOT inheriting commands" in str(w.message)


from intelanalytics.core.metaprog import CommandLoadable, get_doc_stubs_module_text
from intelanalytics.rest.jsonschema import get_command_def

ignore_loadables = ['BigFrame', 'BigGraph']  # these are being deprecated

loadables = dict([(item.__name__, item)
                  for item in ia.__dict__.values()
                  if inspect.isclass(item)
                  and issubclass(item, CommandLoadable)
                  and item.__name__ not in ignore_loadables])


cmd = os.path.join(here, r'../bin/engine-spark.sh')
print "Calling engine-spark to dump the command json file: %s" % cmd
subprocess.call(cmd)

# Get the command definitions, which should have been dumped by the engine-spark's CommandDumper
print "Opening dump file and pulling in the command defintions"
with open("../target/command_dump.json", 'r') as json_file:
    command_defs = [get_command_def(json_schema) for json_schema in json.load(json_file)['commands']]

text = get_doc_stubs_module_text(command_defs, loadables)
if not text:
    print "No doc stub text found, so no file content to write"
    print "Early exit"
else:
    with open(file_name, 'w') as doc_stubs_file:
        print "Writing file %s" % file_name
        doc_stubs_file.write(text)
        print "Complete"



