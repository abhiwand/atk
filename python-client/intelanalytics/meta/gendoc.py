from intelanalytics.meta.metaprog2 import indent, get_type_name


def gen_doc(command_def):
    """return text for a docstring needed for interactive documentation, uses numpy format"""
    # could switch to google-style, which is more concise is quite readable, allows rst markup
    one_line = command_def.doc.one_line
    extended = command_def.doc.extended_summary
    doc = """%s

%s
""" % (one_line, extended)

    params = command_def.parameters
    if params:
        params_text = """
Parameters
==========
"""
        params_text += "\n".join([get_parameter_text(p) for p in params if not p.use_self])
        doc += params_text
    if command_def.return_info:
        doc += get_returns_text(command_def.return_info)

    return doc


def get_parameter_text(p):
    x = "%s: %s" % (p.name, get_type_name(p.data_type))
    if p.optional:
        x += " (default=%s)" % (p.default if p.default is not None else "None")
    x += ("\n" + indent(p.doc))
    return x


def get_returns_text(return_info):
    x = """

Returns
=======
 : %s
""" % (get_type_name(return_info.data_type))
    x += indent(return_info.doc)
    return x
