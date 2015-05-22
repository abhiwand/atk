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
Library for creating pieces of rst text for the REST API, based on metaprog
"""

from intelanalytics.meta.names import upper_first, indent, get_type_name
from intelanalytics.doc.pyrst import get_member_rst_list, get_maturity_rst, is_name_private


def get_command_def_rest_rst(command_def):
    """return rest entry for a function or attribute described by the command def"""
    one_line = command_def.doc.one_line
    if command_def.maturity:
        one_line = get_maturity_rst(command_def.maturity) + "\n" + one_line
    extended = command_def.doc.extended
    arguments = indent("\n".join([_get_argument_rest_rst(p) for p in command_def.parameters]))
    title = ':doc:`Commands <index>` %s' % command_def.full_name
    title_emphasis = "-" * len(title)
    return_info=_get_returns_rest_rst(command_def.return_info)
    command_rest_template = """

{title_emphasis}
{title}
{title_emphasis}

{one_line}

POST /v1/commands/
==================

GET /v1/commands/:id
====================

Request
-------

**Route** ::

  POST /v1/commands/

**Body**

:name:

    {full_name}

:arguments:

{arguments}

|

**Headers** ::

  Authorization: test_api_key_1
  Content-type: application/json
|

**Description**

{extended}

|

Response
--------

**Status** ::

  200 OK

**Body**

Returns information about the command.  See the Response Body for Get Command here below.  It is the same.


GET /v1/commands/:id
====================

Request
-------

**Route** ::

  GET /v1/commands/18

**Body**

(None)

**Headers** ::

  Authorization: test_api_key_1
  Content-type: application/json
|

Response
--------

**Status** ::

  200 OK

**Body**

{return_info}

""".format(title=title,
           title_emphasis=title_emphasis,
           one_line=one_line,
           full_name=command_def.full_name,
           arguments=arguments,
           extended=extended,
           return_info=return_info)
    return command_rest_template


def _get_argument_rest_rst(p):
    data_type = get_type_name(p.data_type)
    doc = p.doc or '<Missing Description>'
    if p.optional:
        data_type += " (default=%s)" % (p.default if p.default is not None else "None")
    return """
**{name}** : {data_type}

..

{description}

""".format(name=p.name, data_type=data_type, description=indent(doc))


def _get_returns_rest_rst(return_info):
    return """
``{data_type}``

{description}
""".format(data_type=get_type_name(return_info.data_type), description=indent(return_info.doc, 8)) if return_info\
        else "<Missing Return Information>"


def get_command_rest_rst_file_name(command_def):
    return command_def.full_name.replace(':', '-').replace('/', '__') + ".rst"

ABOUT_COMMAND_NAMES = "about_command_names"  # for about_command_names.rst

def get_commands_rest_index_content(command_defs):
    return """
:doc:`REST API <../index>` Commands
===================================

.. toctree::
    :maxdepth: 1

    Issue Command <issue_command.rst>
    Get Command <get_command.rst>

.. toctree::
    :hidden:

{manual_hidden_toctree}
{auto_hidden_toctree}

------

Command List
------------


""".format(manual_hidden_toctree=_get_manual_hidden_toctree(),
           auto_hidden_toctree=_get_auto_hidden_toctree(command_defs)) + _get_commands_rest_summary_table(command_defs)


def _get_auto_hidden_toctree(command_defs):
    return indent("\n".join(sorted([get_command_rest_rst_file_name(c)[:-4] for c in command_defs])))


def _get_manual_hidden_toctree():
    return indent("\n".join([ABOUT_COMMAND_NAMES]))


def _get_commands_rest_summary_table(command_defs):
    """Creates rst summary table for given class"""
    name_max_len = 0
    summary_max_len = 0
    line_tuples = []
    for c in command_defs:
        if not is_name_private(c.name):
            doc_ref = get_command_rest_rst_file_name(c)[:-4]  # remove the ".rst"
            name = ":doc:`%s <%s>` " % (c.full_name, doc_ref)
            summary = c.doc.one_line
            if c.maturity:
                summary = get_maturity_rst(c.maturity) + " " + summary
            if len(name) > name_max_len:
                name_max_len = len(name)
            if len(summary) > summary_max_len:
                summary_max_len = len(summary)
            line_tuples.append((name, summary))

    name_len = name_max_len + 2
    summary_len = summary_max_len + 2

    table_line = ("=" * name_len) + "  " + ("=" * summary_len)
    header_command_name = "Command Name  (explained :doc:`here <%s>`)" % ABOUT_COMMAND_NAMES
    table_header = "\n".join([table_line, "%s%s  Description" % (header_command_name, " " * (name_len - len(header_command_name))), table_line])

    lines = sorted(["%s%s  %s" % (t[0], " " * (name_len - len(t[0])), t[1]) for t in line_tuples])
    lines.insert(0, table_header)
    lines.append(table_line)
    return "\n".join(lines)
