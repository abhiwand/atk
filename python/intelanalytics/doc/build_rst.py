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
Builds .rst files and folder structure for Python API
"""

import tempfile
import shutil
import os
import errno
import sys


this_script_dir = os.path.dirname(os.path.abspath(__file__))
source_code_dir = os.path.join(os.path.join(os.path.join(this_script_dir, os.pardir), os.pardir), os.pardir)
dst_path = os.path.join(source_code_dir, "doc/source/python_api")

# override the python path so that 'this' intelanalytics package is used
sys.path.insert(0, os.path.join(source_code_dir, "python"))

# connect to running server
import intelanalytics as ia
ia.connect()


from intelanalytics.meta.metaprog2 import get_installation, get_type_name, has_entity_collection, get_entity_collection
from intelanalytics.meta.genrst import gen_rst


COLLECTION_MARKER_FILE_NAME = ".collection"  # an empty file with this name is created in every folder to mark a collection


def get_prepared_root_path():
    """creates a temporary folder which functions as the root_path for rst construction"""
    tmp_folder_path = tempfile.mkdtemp(prefix="tmp_doc_staging_")
    root_path = os.path.join(tmp_folder_path, "python_api")
    shutil.copytree("api_template", root_path)  # copy the template into the tmp folder
    return root_path


def copy_root_path_to_dst(root_path):
    """Copies the temp folder to destination folder and deletes the temp folder"""
    print "dst_path=%s" % dst_path  # dst_path is global
    if os.path.exists(dst_path):
        print "deleting %s" % dst_path
        shutil.rmtree(dst_path)
    shutil.copytree(root_path, dst_path)


def write_rst_dummy_collection_index_file(path):
    file_path = os.path.join(path, COLLECTION_MARKER_FILE_NAME)
    if not os.path.exists(file_path):
        print "writing %s" % file_path
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


def get_rst_folder_path(root_path, obj, attr=None):
    installation = get_installation(obj, None)
    if installation:
        entity_collection_name = installation.install_path.entity_collection_name
        install_path = installation.install_path.full.replace(':', '-')
    elif attr and has_entity_collection(attr):
        entity_collection_name = get_entity_collection(attr)
        install_path = ''
    else:
        raise RuntimeError("Unable to determine the entity collection for method %s" % attr)

    collection_path = os.path.join(root_path, entity_collection_name)
    if not os.path.exists(collection_path):
        raise RuntimeError("Documentation collections folder %s does not exist" % collection_path)
    write_rst_dummy_collection_index_file(collection_path)
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


def gen_rst_cls_content(cls):
    from intelanalytics.meta.genrst import get_rst_for_class
    return get_rst_for_class(cls)


def get_index_ref(cls):
    installation = get_installation(cls, None)
    nested_level = len(installation.install_path._intermediate_names) if installation else 0
    return ("../" * nested_level) + 'index'


def write_rst_attr_file(root_path, cls, attr):
    command_def = attr.command
    header = get_rst_attr_file_header(class_name=get_type_name(cls), index_ref=get_index_ref(cls), attr_name=attr.__name__) #command_def.name)
    content = gen_rst(command_def)
    folder = get_rst_folder_path(root_path, cls, attr)
    file_path = os.path.join(folder, command_def.name + ".rst")
    print "writing %s" % file_path
    with open(file_path, 'w') as f:
        f.writelines([header, content])


def write_rst_cls_file(root_path, cls):
    installation = get_installation(cls)
    entity_collection_name = installation.install_path.entity_collection_name
    header = get_rst_cls_file_header(entity_collection_name, class_name=get_type_name(cls))
    content = gen_rst_cls_content(cls)
    folder = get_rst_folder_path(root_path, cls)
    file_path = os.path.join(folder, "index.rst")
    print "writing %s" % file_path
    with open(file_path, 'w') as f:
        f.writelines([header, content])


def write_rst_collections_file(root_path, collection_name, subfolders, files):
    # go through subfolders and find the index.rst and them to toc jazz
    # go through files and list global methods  (leave making nice "summary table" as another exercise)
    from intelanalytics.meta.classnames import upper_first, entity_type_to_class_name
    from intelanalytics.meta.metaprog2 import indent
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


def walk_collection_folders(root_path):
    for folder, subfolders, files in os.walk(root_path):
        if COLLECTION_MARKER_FILE_NAME in files:
            write_rst_collections_file(folder, os.path.split(folder)[1], subfolders, files)


def per_attribute(doc_root_path):
    root_path = doc_root_path
    def write_command(obj, attr):
        if hasattr(attr, "command"):
            write_rst_attr_file(root_path, obj, attr)
    return write_command


def per_class(doc_root_path):
    root_path = doc_root_path
    def write_class(cls):
        write_rst_cls_file(root_path,  cls)
    return write_class


def print_attr(obj, attr):
    print "attr:  %s:%s" % (obj, attr)


def print_class(obj):
    print "---------------------------------------------------------"
    print "class: %s" % (obj)
    print "---------------------------------------------------------"



doc_stubs_text = {}   # obj -> list of docstubs per method
                      # ex.
                      #  { Frame -> [ "@docstub\ndef add_columns....", "@docstub\ndef ecdf(..." ...) ]
                      #    VertexFrame -> [ "...  " ...]

from intelanalytics.meta.genspa import gen_spa
from intelanalytics.meta.metaprog2 import get_function_text

# def get_attr_spa(obj_spa, stubs_table):
#     table = stubs_table
#     def attr_spa(obj, attr):
#         if hasattr(attr, "command"):
#             if obj not in table:
#                 obj_spa(obj)
#             command_def = attr.command
#             text = get_function_text(command_def, decorator_text='@doc_stub')
#             table[obj].append(text)
#     return attr_spa
#
# def get_obj_spa(stubs_table):
#     table = stubs_table
#     def obj_spa(obj):
#         if obj not in table:
#             table[obj] = []
#     return obj_spa


def attr_spa(obj, attr):
    if hasattr(attr, "command"):
        if obj not in doc_stubs_text:
            obj_spa(obj)
        command_def = attr.command
        text = get_function_text(command_def, decorator_text='@doc_stub')
        doc_stubs_text[obj].append((command_def.name, text))

def obj_spa(obj):
    if obj not in doc_stubs_text:
        doc_stubs_text[obj] = []

from intelanalytics.meta.metaprog2 import delete_docstubs, get_file_header_text, get_doc_stubs_class_name, CommandInstallable, get_loadable_class_text, indent


def get_doc_stubs_modules_text(): #command_defs, global_module):
    """creates docstub text for two different files, returning a tuple of the file content"""
    # the content of the second item in the tuple contains those entity classes that were created by the metaprogramming, not coded in the python client.
    #install_server_commands(command_defs)
    delete_docstubs()
    before_lines = [get_file_header_text()]
    after_lines = []
    after_all = []
    after_definitions = []
    after_dependencies = {}
    global_lines = []
    #classes = sorted(_installable_classes_store.items(), key=lambda kvp: len(kvp[0]))
    #if 0 < logger.level <= logging.INFO:
        #logger.info("Processing class in this order: " + "\n".join([str(c[1]) for c in classes]))

    classes = sorted(doc_stubs_text.items(), key=lambda kvp: len(kvp[0].__name__))
    for cls, members_info in classes:
        names, text = zip(*members_info)
        installation = get_installation(cls, None)
        if installation:
        #members_text = get_members_text(cls) or indent("pass")
            class_name, baseclass_name = installation.install_path.get_class_and_baseclass_names()
            if class_name != cls.__name__:
                raise RuntimeError("Internal Error: class name mismatch generating docstubs (%s != %s)" % (class_name, cls.__name__))
            if installation.host_class_was_created and installation.install_path.is_entity:
                lines = after_lines
                after_definitions.append(class_name)
                after_dependencies[baseclass_name] = installation.install_path.baseclass_install_path
                #if installation.install_path.is_entity:
                    # need to export it as part of __all__
                after_all.append(class_name)
            else:
                if not installation.host_class_was_created:
                    class_name = get_doc_stubs_class_name(class_name)
                    if baseclass_name != CommandInstallable.__name__:
                        baseclass_name = get_doc_stubs_class_name(baseclass_name)
                lines = before_lines


            lines.append(get_loadable_class_text(class_name,
                                                 "object", #baseclass_name,
                                                 "Contains commands for %s provided by the server" % class_name,
                                                 indent("\n\n".join(text))))
        elif cls.__name__ == "intelanalytics":
            global_lines.extend(list(text))
            after_all.extend(list(names))

    for d in after_definitions:
        after_dependencies.pop(d, None)
        # for baseclass_name, install_path in after_dependencies.items():
        #     if install_path:
    #         module_path = _installable_classes_store[install_path.full].__module__
    #         after_lines.insert(0, "from %s import %s" % (module_path, baseclass_name))
    # global_lines, global_all = get_doc_stub_globals_text(global_module)
    after_lines.extend(global_lines)
    #after_all.extend(global_all)
    after_lines.insert(0, '__all__ = ["%s"]' % '", "'.join(after_all))  # export the entities created in 'after'
    after_lines.insert(0, get_file_header_text())
    return '\n'.join(before_lines), '\n'.join(after_lines)


def main():
    rp = get_prepared_root_path()
    print "Using root_path=%s" % rp
    #ia._walk_api(per_class(rp), per_attribute(rp), include_init=True)
    #obj_spa = get_obj_spa(doc_stubs_text)
    #attr_spa = get_attr_spa(obj_spa, doc_stubs_text)
    ia._walk_api(obj_spa, attr_spa, include_init=False)

    # write the index.rst files for the collection folders...
    # walk and look for .collection files
    walk_collection_folders(rp)

    copy_root_path_to_dst(rp)
    print "Done."



main()
#print str(doc_stubs_text)
#print get_doc_stubs_modules_text()

text_1, text_2 = get_doc_stubs_modules_text() #command_defs, ia)


def write_text(file_name, text):
    with open(file_name, 'w') as doc_stubs_file:
        print "Writing file %s" % file_name
        doc_stubs_file.write(text)

import os
root_path = os.path.dirname(ia.__file__) + "/core/"
print root_path
write_text(root_path + "docstubs1.py", text_1)
write_text(root_path + "docstubs2.py", text_2)

print "Complete"
