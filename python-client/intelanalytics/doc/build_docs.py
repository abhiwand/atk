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
Script to generate all the collateral needed for documentation and Static Program Analysis (SPA)

Builds .rst files and folder structure for Python API HTML+PDF documentation

Builds .rst files and folder structure for REST API HTML+PDF documentation

Builds docstub*.py files for SPA
"""

# todo: break this script up a little, move to genrst.py and genspa.py

import tempfile
import shutil
import os
import errno
import sys
import glob
import logging
logger = logging.getLogger(__name__)


this_script_dir = os.path.dirname(os.path.abspath(__file__))
source_code_dir = os.path.join(os.path.join(os.path.join(this_script_dir, os.pardir), os.pardir), os.pardir)
doc_source_dir = os.path.join(source_code_dir, "doc/source")
python_api = "python_api"
rest_api = "rest_api"
dst_python_api_dir = os.path.join(doc_source_dir, python_api)
dst_rest_api_dir = os.path.join(doc_source_dir, rest_api)
dst_rest_api_commands_dir = os.path.join(dst_rest_api_dir, "v1/commands")
dst_docstubs_dir = os.path.join(source_code_dir, r'python-client/intelanalytics/core')

# override the python path so that 'this' intelanalytics package is used
sys.path.insert(0, os.path.join(source_code_dir, "python"))


spa_module1_file_name = os.path.join(dst_docstubs_dir, 'docstubs1.py')
spa_module2_file_name = os.path.join(dst_docstubs_dir, 'docstubs2.py')


def delete_existing_docstubs():
    for file_name in [spa_module1_file_name, spa_module2_file_name]:
        for existing_doc_file in glob.glob("%s*" % file_name):  # * on the end to get the .pyc as well
            print "Deleting existing docstub file: %s" % existing_doc_file
            os.remove(existing_doc_file)

# Delete any existing docstub.py files BEFORE importing ia
delete_existing_docstubs()

# Connect to running server
import intelanalytics as ia
print "Using package: %s" % ia.__file__
ia.connect()

from intelanalytics.meta.metaprog2 import get_installation, get_type_name, has_entity_collection, get_entity_collection, get_function_text
from intelanalytics.meta.metaprog2 import get_file_header_text, get_file_footer_text, get_doc_stubs_class_name, get_loadable_class_text, indent, get_doc_stub_init_text
from intelanalytics.meta.genrst import get_command_def_rst, get_class_rst
from intelanalytics.meta.genrest import get_command_def_rest_rst

#############################################################################
# python rst

COLLECTION_MARKER_FILE_NAME = ".collection"  # an empty file with this name is created in a folder to mark it as  a collection

def delete_folder(path):
    if os.path.exists(path):
        print "Deleting folder %s" % path
        shutil.rmtree(path)


def copy_template(src_template_name):
    """copies folder to folder of same name in the doc/source dir"""
    dst_path = os.path.join(doc_source_dir, src_template_name)
    print "Copying %s to %s" % (src_template_name, dst_path)
    shutil.copytree(src_template_name, dst_path)


def write_dummy_collection_index_file(path):
    file_path = os.path.join(path, COLLECTION_MARKER_FILE_NAME)
    if not os.path.exists(file_path):
        print "Writing %s" % file_path
        with open(file_path, 'w') as f:
            f.write("# Dummy file marking this folder as a entity collection folder")


def ensure_folder_exists(folder_path):
    try:
        os.makedirs(folder_path)
    except OSError as ex:
        if ex.errno != errno.EEXIST:
            raise


def split_path(path):
    """splits a path into a list of strings"""
    folders = []
    while 1:
        path, folder = os.path.split(path)
        if folder != "":
            folders.append(folder)
        if path == '':
            break
    folders.reverse()
    return folders


def get_entity_collection_name_and_install_path(obj, attr):
    installation = get_installation(obj, None)
    if installation:
        entity_collection_name = installation.install_path.entity_collection_name
        install_path = installation.install_path.full.replace(':', '-')
    elif attr and has_entity_collection(attr):
        entity_collection_name = get_entity_collection(attr)
        install_path = ''
    else:
        raise RuntimeError("Unable to determine the entity collection for method %s" % attr)
    return entity_collection_name, install_path


def get_rst_folder_path(rst_root_path, obj, attr=None):
    entity_collection_name, install_path = get_entity_collection_name_and_install_path(obj, attr)

    collection_path = os.path.join(rst_root_path, entity_collection_name)
    if not os.path.exists(collection_path):
        raise RuntimeError("Documentation collections folder %s does not exist" % collection_path)
    write_dummy_collection_index_file(collection_path)
    install_path_parts = split_path(install_path)
    path = collection_path
    for part in install_path_parts:
        path = os.path.join(path, part)
        ensure_folder_exists(path)
    return path


def get_rst_attr_file_header(class_name, index_ref, attr_name):
    """produce the rst content to begin an attribute-level *.rst file"""
    # use  :doc:`class_name<index>` syntax to create reference back to the index.rst file
    title = ":doc:`%s <%s>`  %s" % (class_name, index_ref, attr_name)
    return get_rst_file_header(title)


def get_rst_file_header(title):
    title_emphasis = '*' * len(title)
    return """
{title}
{title_emphasis}

------

""".format(title=title, title_emphasis=title_emphasis)


def get_rst_cls_file_header(collection_name, class_name):
    """produce the rst content to begin an attribute-level *.rst file"""
    # use  :doc:`class_name<index>` syntax to create reference back to the index.rst file
    title = ":doc:`%s<../index>` %s" % (collection_name.capitalize(), class_name)
    return get_rst_file_header(title)


def get_index_ref(cls):
    installation = get_installation(cls, None)
    nested_level = len(installation.install_path._intermediate_names) if installation else 0
    return ("../" * nested_level) + 'index'


def write_py_rst_attr_file(root_path, cls, attr):
    command_def = attr.command
    header = get_rst_attr_file_header(class_name=get_type_name(cls), index_ref=get_index_ref(cls), attr_name=attr.__name__) #command_def.name)
    content = get_command_def_rst(command_def)
    folder = get_rst_folder_path(root_path, cls, attr)
    file_path = os.path.join(folder, command_def.name + ".rst")
    print "writing %s" % file_path
    with open(file_path, 'w') as f:
        f.writelines([header, content])


def write_rest_rst_attr_file(root_path, cls, attr):
    command_def = attr.command
    content = get_command_def_rest_rst(cls.__name__, command_def)
    folder = get_rst_folder_path(root_path, cls, attr)
    file_path = os.path.join(folder, command_def.name + ".rst")
    print "writing %s" % file_path
    with open(file_path, 'w') as f:
        f.writelines([content])


def write_py_rst_cls_file(root_path, cls):
    installation = get_installation(cls)
    entity_collection_name = installation.install_path.entity_collection_name
    header = get_rst_cls_file_header(entity_collection_name, class_name=get_type_name(cls))
    content = get_class_rst(cls)
    folder = get_rst_folder_path(root_path, cls)
    file_path = os.path.join(folder, "index.rst")
    print "writing %s" % file_path
    with open(file_path, 'w') as f:
        f.writelines([header, content])


def write_rest_rst_cls_file(root_path, cls):
    installation = get_installation(cls)
    entity_collection_name = installation.install_path.entity_collection_name
    header = get_rst_cls_file_header(entity_collection_name, class_name=get_type_name(cls))
    content = get_rest_summary_table(cls)
    folder = get_rst_folder_path(root_path, cls)
    file_path = os.path.join(folder, "index.rst")
    print "writing %s" % file_path
    with open(file_path, 'w') as f:
        f.writelines([header, content])

def write_py_rst_collections_file(root_path, collection_name, subfolders, files):
    # go through subfolders and find the index.rst and add to toc jazz
    # go through files and list global methods  (leave making nice "summary table" as another exercise)
    from intelanalytics.meta.classnames import upper_first, entity_type_to_class_name, indent
    title = upper_first(collection_name)
    title_emphasis = "=" * len(title)
    # Frame <frame-/index.rst>
    toctree = indent("\n".join(["%s <%s/index.rst>" % (entity_type_to_class_name(subfolder.replace('-',':')), subfolder) for subfolder in subfolders]))
    globs = "\n\n".join([":doc:`%s<%s>`" % (f[:-4], f[:-4]) for f in files if f[-4:] == ".rst" and f != "index.rst"])
    content =  """
{title}
{title_emphasis}

.. toctree::
{toctree}

-------

**Global Methods**

{globs}
""".format(title=title, title_emphasis=title_emphasis, toctree=toctree, globs=globs)
    file_path = os.path.join(root_path, "index.rst")
    print "writing %s" % file_path
    with open(file_path, 'w') as f:
        f.write(content)


def get_rest_summary_table(cls):
    from intelanalytics.meta.genrst import get_member_rst_list, get_maturity_rst, is_name_private
    members = get_member_rst_list(cls)
    name_max_len = 0
    summary_max_len = 0
    line_tuples = []
    for m in members:
        if not is_name_private(m.display_name) and m.display_name != "__init__":
            display_name = m.display_name.replace('.', '/')
            name = ":doc:`%s <%s>`\ " % (display_name, display_name)
            summary = m.doc.one_line
            if m.maturity:
                summary = get_maturity_rst(m.maturity) + " " + summary
            if len(name) > name_max_len:
                name_max_len = len(name)
            if len(summary) > summary_max_len:
                summary_max_len = len(summary)
            line_tuples.append((name, summary))

    name_len = name_max_len + 2
    summary_len = summary_max_len + 2

    table_line = ("=" * name_len) + "  " + ("=" * summary_len)

    lines = sorted(["%s%s  %s" % (t[0], " " * (name_len - len(t[0])), t[1]) for t in line_tuples])
    lines.insert(0, table_line)
    lines.append(table_line)
    return "\n".join(lines)


def write_rest_rst_collections_file(root_path, collection_name, subfolders, files):
    # go through subfolders and add to toc jazz for the classes in the collections.  For REST API, we could flatten this
    from intelanalytics.meta.classnames import upper_first, entity_type_to_class_name, indent
    title = ":doc:`Commands <../index>`  %s" % upper_first(collection_name)
    title_emphasis = "=" * len(title)
    toctree = indent("\n".join(["%s <%s/index.rst>" % (entity_type_to_class_name(subfolder.replace('-',':')), subfolder) for subfolder in subfolders]))
    content =  """
{title}
{title_emphasis}

.. toctree::
{toctree}

""".format(title=title, title_emphasis=title_emphasis, toctree=toctree)
    file_path = os.path.join(root_path, "index.rst")
    print "writing %s" % file_path
    with open(file_path, 'w') as f:
        f.write(content)


def walk_py_rst_collection_folders(root_path):
    for folder, subfolders, files in os.walk(root_path):
        if COLLECTION_MARKER_FILE_NAME in files:
            write_py_rst_collections_file(folder, os.path.split(folder)[1], subfolders, files)


def walk_rest_rst_collection_folders(root_path):
    for folder, subfolders, files in os.walk(root_path):
        if COLLECTION_MARKER_FILE_NAME in files:
            write_rest_rst_collections_file(folder, os.path.split(folder)[1], subfolders, files)


def get_attr_py_rst(doc_root_path):
    root_path = doc_root_path
    def write_command(obj, attr):
        if hasattr(attr, "command"):
            write_py_rst_attr_file(root_path, obj, attr)
    return write_command

def get_attr_rest_rst(doc_root_path):
    root_path = doc_root_path
    def write_command(obj, attr):
        if hasattr(attr, "command"):
            write_rest_rst_attr_file(root_path, obj, attr)
    return write_command


def get_obj_py_rst(doc_root_path):
    root_path = doc_root_path
    def write_class(cls):
        write_py_rst_cls_file(root_path,  cls)
    return write_class


def get_obj_rest_rst(doc_root_path):
    root_path = doc_root_path
    def write_class(cls):
        write_rest_rst_cls_file(root_path,  cls)
    return write_class



##############################################################################
# SPA

spa_obj_to_member_text = {}   # obj -> list of docstub text per method, ex.
                              #  { Frame -> [ "@docstub\ndef add_columns....", "@docstub\ndef ecdf(..." ...) ],
                              #    VertexFrame -> [ "...  " ...], ... }


spa_import_return_types = set()  # for spa to work (at least in IJ), the rtype must be in the modules namespace, like
# for example, get_frame, returns a Frame.  For IJ to provide metadata about the return type, Frame has to be around
# so we hold on to all the funky return types so we can import them in the docstubs.


def attr_spa(obj, attr):
    if hasattr(attr, "command"):
        if obj not in spa_obj_to_member_text:
            obj_spa(obj)
        command_def = attr.command
        if command_def.is_constructor:
            text = get_doc_stub_init_text(command_def, override_rtype=obj)
        else:
            text = get_function_text(command_def, decorator_text='@doc_stub')
        if command_def.return_info:
            return_type = command_def.return_info.data_type
            if return_type:
                s = return_type.__name__ if hasattr(return_type, "__name__") else str(return_type) if not isinstance(return_type, basestring) else return_type
                if s not in dir(__builtins__) and s not in spa_import_return_types:
                    spa_import_return_types.add(return_type)
        spa_obj_to_member_text[obj].append((command_def.name, text))


def obj_spa(obj):
    if obj not in spa_obj_to_member_text:
        spa_obj_to_member_text[obj] = []


def get_spa_modules_text():
    """creates spa text for two different modules, returning the content in a tuple"""

    # The first module contains dependencies for the entity classes that are 'hard-coded' in the python API,
    # like Frame, Graph...  They need things like  _DocStubsFrame, or GraphMl to be defined first.
    # The second module contains entity classes that are created by meta-programming, like LdaModel, *Model,
    # which may depend on the 'hard-coded' python API.  The second modules also contains any global methods,
    # like get_frame_names, which depend on objects like Frame being already defined.

    module1_lines = [get_file_header_text()]

    module2_lines = []
    module2_all = []  # holds the names which should be in module2's __all__ for import *

    classes = sorted([(k, v) for k, v in spa_obj_to_member_text.items()], key=lambda kvp: kvp[0].__name__)
    for cls, members_info in classes:
        logger.info("Processing %s for doc stubs", cls)
        names, text = zip(*members_info)
        installation = get_installation(cls, None)
        if installation:
            class_name, baseclass_name = installation.install_path.get_class_and_baseclass_names()
            if class_name != cls.__name__:
                raise RuntimeError("Internal Error: class name mismatch generating docstubs (%s != %s)" % (class_name, cls.__name__))
            if installation.host_class_was_created and installation.install_path.is_entity:
                lines = module2_lines
                module2_all.append(class_name)
            else:
                if not installation.host_class_was_created:
                    class_name = get_doc_stubs_class_name(class_name)
                lines = module1_lines

            lines.append(get_loadable_class_text(class_name,
                                                 "object",  # no inheritance for docstubs, just define all explicitly
                                                 "Auto-generated to contain doc stubs for static program analysis",
                                                 indent("\n\n".join(text))))
        elif cls.__name__ == "intelanalytics":
            module2_lines.extend(list(text))
            module2_all.extend(list(names))

    module2_lines.insert(0, '\n__all__ = ["%s"]' % '", "'.join(module2_all))

    # Need to import any return type to enable SPA, like for get_frame, we need Frame
    for t in spa_import_return_types:
        if test_import(t):
            module2_lines.insert(0, "from intelanalytics import %s" % t)

    module2_lines.insert(0, get_file_header_text())

    # remove doc_stub from namespace
    module1_lines.append(get_file_footer_text())
    module2_lines.append("\ndel doc_stub")

    return '\n'.join(module1_lines), '\n'.join(module2_lines)


def test_import(name):
    """Determines if the name is importable from main module"""
    try:
        import intelanalytics as ia
        getattr(ia, name)
        return True
    except:
        return False


def write_text_to_file(file_name, text):
    with open(file_name, 'w') as doc_stubs_file:
        print "Writing file %s" % file_name
        doc_stubs_file.write(text)


delete_folder(dst_python_api_dir)
delete_folder(dst_rest_api_dir)

copy_template(python_api)
path = dst_python_api_dir
print "Creating rst files for Python API docs, using root_path %s" % path
ia._walk_api(get_obj_py_rst(path), get_attr_py_rst(path), include_init=True)
# write the index.rst files for the collection folders...
# walk and look for .collection files
walk_py_rst_collection_folders(path)

copy_template(rest_api)
path = dst_rest_api_commands_dir  # the autogen stuff is just for commands/
print "Creating rst files for REST API docs, using root_path %s" % path
ia._walk_api(get_obj_rest_rst(path), get_attr_rest_rst(path), include_init=True)
# write the index.rst files for the collection folders...
# walk and look for .collection files
walk_rest_rst_collection_folders(path)

print "Creating spa modules..."
ia._walk_api(obj_spa, attr_spa, include_init=True)
text_1, text_2 = get_spa_modules_text()
write_text_to_file(spa_module1_file_name, text_1)
write_text_to_file(spa_module2_file_name, text_2)
print "Done."

