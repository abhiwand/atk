
from intelanalytics.core.frame import BigFrame
from intelanalytics.core.graph import BigGraph
from intelanalytics.core.metaprog import make_function_code_text, make_property_code_text, load_loadable, _loadable_classes
from intelanalytics.rest.jsonschema import get_command_def

import json
import datetime


def indent(text, spaces=4):
    indentation = ' ' * spaces
    return "\n".join([indentation + line for line in text.split('\n')])


def get_auto_text(loadable_class, member_classes):
    lines = []
    lines.extend(get_file_header_lines(loadable_class))
    lines.append('\n')
    lines.extend(get_member_commands_lines(loadable_class))
    lines.extend(get_member_classes_lines(loadable_class, member_classes))
    return "\n".join(lines)


def get_file_header_lines(loadable_class):
    legal = """##############################################################################
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
    doc = '"""\nAuto-generated file for %s commands (%s)\n\n**DO NOT EDIT**\n\"""\n' \
          % (loadable_class.__name__, datetime.datetime.now().isoformat())
    imports = ['from intelanalytics.core.metaprog import CommandLoadable',
               'from intelanalytics.rest.command import execute_command']
    return [legal, doc] + imports


def get_member_commands_lines(loadable_class):
    lines = get_function_lines(loadable_class)
    if lines:
        lines.insert(0, 'class CommandLoadable%s(CommandLoadable):\n    """\n    Contains commands for %s provided by the server\n    """' %  (loadable_class.__name__, loadable_class.__name__))
    return lines


def get_member_classes_lines(loadable_class, member_clasess):
    lines = []
    for c in member_clasess.values():
        if c.__name__.startswith(loadable_class.__name__):
            lines.append('')  # insert extra blank line
            lines.append("class %s(object):" % c.__name__)
            lines.append('    """\n%s\n    """' % indent(c.__doc__))
            lines.extend(get_function_lines(c))
    return lines


def get_function_lines(loadable_class):
    lines = []
    for member_name in sorted(loadable_class.__dict__.keys()):
        member = loadable_class.__dict__[member_name]
        if hasattr(member, "command"):
            json_schema = getattr(member, "command")
            lines.append(indent(make_function_code_text(get_command_def(json_schema))))
        elif type(member) is property and hasattr(member.fget, 'loadable_class'):
            lines.append(indent(make_property_code_text(member_name)))
    return lines

def write_auto_file(filename, text):
    with open(filename, 'w') as f:
        f.write(text)





# start script

with open("../target/command_dump.json", 'r') as f:
   commands = [get_command_def(json_schema) for json_schema in json.load(f)['commands']]

load_loadable(BigFrame, commands, None)
load_loadable(BigGraph, commands, None)

# we grab the _loadable_classes global variable which is conveniently holding
# all the dynamically generated member classes

write_auto_file('intelanalytics/core/autoframe.py', get_auto_text(BigFrame, _loadable_classes))
write_auto_file('intelanalytics/core/autograph.py', get_auto_text(BigGraph, _loadable_classes))

# print get_auto_text(BigGraph, _loadable_classes)
# print "=" * 80
# print get_auto_text(BigFrame, _loadable_classes)
