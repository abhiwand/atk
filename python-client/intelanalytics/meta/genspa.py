from intelanalytics.meta.classnames import indent, get_type_name


def gen_spa(command_def, override_rtype=None):
    """return text for a docstring needed for SPA, uses classic rst format"""
    doc_str = str(command_def.doc)

    params = command_def.parameters
    if params:
        params_text = "\n".join([get_parameter_text(p) for p in params if not p.use_self])
        doc_str += ("\n\n" + params_text)
    if command_def.return_info:
        doc_str += ("\n\n" + get_returns_text(command_def.return_info, override_rtype))

    return indent(doc_str)


def get_parameter_text(p):
    description = indent(p.doc)[4:]  # indents, but grabs the first line's space back
    if p.optional:
        description = "(default=%s)  " % (p.default if p.default is not None else "None") + description

    return ":param {name}: {description}\n:type {name}: {data_type}".format(name=p.name,
                                                                            description=description,
                                                                            data_type=get_type_name(p.data_type))


def get_returns_text(return_info, override_rtype):
    description = indent(return_info.doc)[4:]  # indents, but grabs the first line's space back
    return ":returns: {description}\n:rtype: {data_type}".format(description=description,
                                                                 data_type=get_type_name(override_rtype
                                                                                         or return_info.data_type))
