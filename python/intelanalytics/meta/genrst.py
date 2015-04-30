from intelanalytics.meta.metaprog2 import indent, get_type_name, get_installation, Constants
from intelanalytics.meta.classnames import is_name_private
from intelanalytics.meta.reflect import get_args_text_from_function

import re
from collections import OrderedDict
from intelanalytics.rest.jsonschema import Doc


def gen_rst(command_def):
    """return text for a docstring needed for interactive documentation, uses numpy format"""
    # could switch to google-style, which is more concise is quite readable, allows rst markup
    one_line = command_def.doc.one_line
    extended = command_def.doc.extended
    signature = command_def.get_function_parameters_text()
    doc = """
.. function:: {name}({signature})

{one_line}


{extended}
""".format(name=command_def.function_name, signature=signature, one_line=indent(one_line), extended=indent(extended))


    if command_def.parameters:
        doc += indent(get_parameters_text(command_def))
    if command_def.return_info:
        doc += indent(get_returns_text(command_def.return_info))
    return doc


def get_parameters_text(command_def):
    return """
:Parameters:
%s
""" % indent("\n".join([get_parameter_text(p) for p in command_def.parameters if not p.use_self]))


def get_parameter_text(p):
    data_type = get_type_name(p.data_type)
    if p.optional:
        data_type += " (default=%s)" % (p.default if p.default is not None else "None")
    return """
**{name}** : {data_type}

..

{description}

""".format(name=p.name, data_type=data_type, description=indent(p.doc))


def get_returns_text(return_info):
    return """

:Returns:

    : {data_type}

    ..

{description}
""".format(data_type=get_type_name(return_info.data_type), description=indent(return_info.doc, 8))


"""
:Parameters:

            **rules** : list of Rule (optional)

            ..

                list of rules which specify how the graph will be created; if empty
                an empty graph will be created

            **name** : str (optional)

"""


###########################################################################
# code from metaprog2.py that's just for rst

first_column_header_char_count = 100
second_column_header_char_count = 100
number_of_spaces_between_columns = 2
index_of_summary_start = first_column_header_char_count + number_of_spaces_between_columns
table_line = "=" * first_column_header_char_count + " " * number_of_spaces_between_columns + "=" * second_column_header_char_count


def _get_method_summary_table_tuple(command_def):
    name = command_def.function_name
    if command_def.intermediates:
        name = '.'.join(list(command_def.intermediates) + [name])
    signature = command_def.get_function_parameters_text()
    summary = command_def.one_line  #get_function_summary(command_def)
    return name, signature, summary


def get_signature_max(rst_name):
    return first_column_header_char_count - len(rst_name)


def get_method_summary_table(sorted_members, cls):
    lines = ["\n.. rubric:: Methods", "", table_line]
    for member in sorted_members:
        if member.in_method_table:
            lines.append(member.get_summary_table_entry(cls))
    lines.append(table_line)
    return "\n".join(lines)

def get_attribute_summary_table(sorted_members, cls):
    lines = ["\n.. rubric:: Attributes", "", table_line]
    for member in sorted_members:
        if member.in_attribute_table:
            lines.append(member.get_summary_table_entry(cls))
    lines.append(table_line)
    return "\n".join(lines)


def get_signature_for_summary(rst_name, sig):
    if not sig:
        sig = ''
    else:
        #max_chars = max(10, sig_col_len - len(display_name) - 2)  # -2 for the ()'s
        sig = mangle_signature(sig, max_chars=get_signature_max(rst_name))
        sig = sig.replace('*', r'\*')
    return sig


def get_function_summary(command_def):
    return command_def.doc.one_line


def get_member_rst_list(cls, intermediate_prefix=None):
    prefix = intermediate_prefix if intermediate_prefix else ''
    # create a ordered dict of names -> whether the name is inherited or not, based on do dir() vs. __dict__ comparison
    members = OrderedDict([(name, name not in cls.__dict__) for name in dir(cls) if not is_name_private(name)])  # True means we assume all are inherited
    # for k in cls.__dict__.keys():
    #     if k in members:
    #         members[k] = False  # i.e. mark NOT inherited
    #     elif not k.startswith("_"):
    #         raise RuntimeError("__dict__ member %s not in dir() for class %s" % (k, cls))

    # Actually, we don't need the command_defs, can just leverage work they've already done by directly interrogating the method
    #  X create a dict from command defs with display name key and function text

    member_rst_list = [MemberRst(cls, getattr(cls, name), prefix + name, is_inherited) for name, is_inherited in members.items()]
    #rst_members = [get_member_rst(prefix + name, is_inherited, getattr(cls, name)) for name, is_inherited in members.items()]

    # what about intermediates and their inheritance --duplicate these?  sure, for now
    installation = get_installation(cls, None)
    if installation:
        for property_name, intermediate_cls in installation.intermediates.items():
            p = prefix + property_name + '.'
            member_rst_list.extend(get_member_rst_list(intermediate_cls, intermediate_prefix=p))  # recursion

    return sorted(member_rst_list, key=lambda m: m.display_name)

    # i don't think we can use autofunction :( what a waste, because we still need the signature and summary for the summary table
    # so I have to spin the signature discovery stuff myself :^( in the cases where we do not have a command_def


def get_class_rst(cls):
    template = """
.. class:: {name}

{rst_doc}""".format(name=get_type_name(cls), rst_doc=indent(cls.__doc__))

    return template


def get_member_details(sorted_members):
    lines = []
    for d in sorted_members:
        lines.append(d.get_rst())
    return "\n".join(lines)


def get_rst_for_class(cls):
    installation = get_installation(cls)
    lines = [get_class_rst(cls)]
    # sort defs for both summary table and
    sorted_members = get_member_rst_list(cls)
    # create attribute summary table
    lines.append(indent(get_attribute_summary_table(sorted_members, cls), 4))
    # create method summary table
    lines.append(indent(get_method_summary_table(sorted_members, cls), 4))

    init_commands = [c for c in installation.commands if c.is_constructor]
    if init_commands:
        lines.append("\n.. _%s:\n" % get_cls_init_rst_label(cls))  # add rst label for internal reference
        lines.append(gen_rst(init_commands[0]))

    return "\n".join(lines)


def doc_to_rst(doc):
    return doc if isinstance(doc, Doc) else get_doc_tuple(doc)


def get_doc_tuple(doc_str):
    if doc_str:
        lines = doc_str.split('\n')

        while lines and not lines[0].strip():
            lines.pop(0)

        # If there's a blank line, then we can assume the first sentence /
        # paragraph has ended, so anything after shouldn't be part of the
        # summary
        for i, piece in enumerate(lines):
            if not piece.strip():
                lines = lines[:i]
                break

        # Try to find the "first sentence", which may span multiple lines
        m = re.search(r"^([A-Z].*?\.)(?:\s|$)", " ".join(lines).strip())
        if m:
            summary = m.group(1).strip()
        elif lines:
            summary = lines[0].strip()
        else:
            summary = ''
        if summary:
            return Doc(summary, doc_str[len(summary):].strip())

    return Doc("<Missing>", doc_str)


def get_cls_init_rst_label(cls):
    return "%s__init__" % get_type_name(cls)

class MemberRst(object):
    def __init__(self, cls, member, display_name, is_inherited, command_def=None):
        is_private = is_name_private(member.fget.__name__ if isinstance(member, property) else member.__name__)
        in_method_table = hasattr(member,  '__call__') and not is_private
        in_attribute_table = not in_method_table and not is_private
        #print "hee, v=%s for display_name=%s" % (v, display_name)
      #  args = None if not in_method_table else get_signature_from_function(member)
      #  return RstMember(display_name, is_inherited, args=args, in_method_table=in_method_table, in_attribute_table=in_attribute_table, doc=member.__doc__)


# class RstMember(object):

    # def __init__(self, display_name, is_inherited, args, in_method_table, in_attribute_table, doc, command_def=None):
        self.cls = cls
        self.display_name = display_name
        self.is_inherited = is_inherited
        self.args = None if not in_method_table else get_args_text_from_function(member)
        self.in_method_table = in_method_table
        self.in_attribute_table = in_attribute_table
        self.doc = doc_to_rst(member.__doc__)
        self.command_def = command_def

    def get_summary_table_entry(self, cls):
        summary = self.doc.one_line

        if self.display_name == "__init__":
            rst_name = ":ref:`__init__ <%s>`\ " % get_cls_init_rst_label(cls)
        else:
            rst_name = ":doc:`%s <%s>`\ " % (self.display_name, self.display_name)

        if self.in_method_table:
            signature = get_signature_for_summary(rst_name, self.args)
        else:
            signature = ''

        first_half = rst_name + signature
        spaces = " " * (index_of_summary_start - len(first_half))
        return first_half + spaces + summary
        #return get_method_summary_entry(self.display_name, signature, summary)

    def get_rst(self):
        return """
.. function:: {name}({args})

{rst_doc}""".format(name=self.display_name, args=self.args, rst_doc=indent(self.doc, 4))



###########################################################################
# rest of the file taken from sphinx/ext/autosummary/__init__.py

max_item_chars = 50


# sig should be args only --i.e. what's inside the parentheses
def mangle_signature(sig, max_chars=30):
    """Reformat a function signature to a more compact form."""
    s = re.sub(r"^\((.*)\)$", r"\1", sig).strip()

    # Strip strings (which can contain things that confuse the code below)
    s = re.sub(r"\\\\", "", s)
    s = re.sub(r"\\'", "", s)
    s = re.sub(r"'[^']*'", "", s)

    # Parse the signature to arguments + options
    args = []
    opts = []

    opt_re = re.compile(r"^(.*, |)([a-zA-Z0-9_*]+)=")
    while s:
        m = opt_re.search(s)
        if not m:
            # The rest are arguments
            args = s.split(', ')
            break

        opts.insert(0, m.group(2))
        s = m.group(1)[:-2]

    # Produce a more compact signature
    sig = limited_join(", ", args, max_chars=max_chars-2)
    if opts:
        if not sig:
            sig = "[%s]" % limited_join(", ", opts, max_chars=max_chars-4)
        elif len(sig) < max_chars - 4 - 2 - 3:
            sig += "[, %s]" % limited_join(", ", opts,
                                           max_chars=max_chars-len(sig)-4-2)

    return u"(%s)" % sig


def limited_join(sep, items, max_chars=30, overflow_marker="..."):
    """Join a number of strings to one, limiting the length to *max_chars*.

    If the string overflows this limit, replace the last fitting item by
    *overflow_marker*.

    Returns: joined_string
    """
    full_str = sep.join(items)
    if len(full_str) < max_chars:
        return full_str

    n_chars = 0
    n_items = 0
    for j, item in enumerate(items):
        n_chars += len(item) + len(sep)
        if n_chars < max_chars - len(overflow_marker):
            n_items += 1
        else:
            break

    return sep.join(list(items[:n_items]) + [overflow_marker])


