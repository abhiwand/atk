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
Formatting for numpydoc docstrings
"""
from intelanalytics import valid_data_types


def get_numpy_doc(command_def, one_line_summary, extended_summary=''):
    return repr(CommandNumpyDoc(command_def, one_line_summary, extended_summary))


class CommandNumpyDoc(object):
    """
    Creates a doc string for the given CommandDefinition according to the numpydoc reStructuredText markup

    (instantiate, grab repr, discard)

    Parameters
    ----------
    command_def : CommandDefinition
        command definition
    one_line_summary : str
        simple, one-line summary
    extended_summary : str (optional)
        more involved summary text
    """
    def __init__(self, command_def, one_line_summary, extended_summary=''):
        self.command_def = command_def
        self.doc = self._create_doc(one_line_summary, extended_summary)

    def __repr__(self):
        return self.doc

    # numpydoc sections for the function:
    #   short_summary
    #   extended_summary
    #   parameters
    #   returns
    #   raises
    #   notes
    #   examples
    #   version

    @staticmethod
    def _get_header(section_name):
        return "\n    %s\n    %s" % (section_name, '-' * len(section_name))

    def _format_summary(self, summary):
        summary = summary.rstrip()
        # make sure it ends with a period
        if summary and summary[-1] != '.':
            summary = summary + "."
        return summary

    def _format_parameters(self):
        items = [self._get_header("Parameters")]
        template = """    %s : %s %s\n%s"""  # $name : $data_type $optional\n$description
        for p in self.command_def.parameters:
            if not p.use_self:
                items.append(template % (p.name,
                                         self._get_data_type_str(p.data_type),
                                         '(optional)' if p.optional else '',
                                         self._indent(p.doc, spaces=8)))
        return "\n".join(items)

    def _format_returns(self):
        template = """%s\n    %s\n%s"""
        return template % (self._get_header("Returns"),
                           self._get_data_type_str(self.command_def.return_type.data_type),
                           self._indent(self.command_def.return_type.doc, spaces=8))

    @staticmethod
    def _indent(text, spaces=4):
        """splits text into lines and indents it"""
        indentation = ' ' * spaces + '%s'
        return "\n".join([indentation % s.strip() for s in text.split("\n")]) if text else ''

    @staticmethod
    def _get_data_type_str(data_type):
        """returns friendly string form of the data_type"""
        try:
            return valid_data_types.to_string(data_type)
        except:
            try:
                return data_type.__name__
            except:
                return str(data_type)

    def _create_doc(self, one_line_summary, extended_summary, ignore_parameters=True):
        """Assembles the doc sections and creates a full doc string for this function"""
        sections = []
        if one_line_summary:
            sections.append(self._format_summary(one_line_summary))
        if extended_summary:
            sections.append(extended_summary)
        # TODO - get the parameter documentation in place and remove ignore_parameters arg
        if not ignore_parameters:
            if self.command_def.parameters:
                sections.append(self._format_parameters())
            if self.command_def.return_type:
                sections.append(self._format_returns())
        # TODO - raises
        # TODO - notes
        # TODO - examples
        sections.append("\n    .. versionadded:: 0.8")  # TODO - enhance version
        return "\n".join(sections)