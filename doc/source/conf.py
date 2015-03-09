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

# -*- coding: utf-8 -*-
#
# IntelAnalytics documentation build configuration file, created by
# sphinx-quickstart on Tue Apr  8 15:19:15 2014.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

import sys
import os

rst_epilog = """

.. |ALPHA|  image:: _static/alpha.*
              :target: glossary.html#term-api-maturity-tags
.. |API|    replace:: abbr:`API (Application Programming Interface)`
.. |BETA|   image:: _static/beta.*
              :target: glossary.html#term-api-maturity-tags
.. |CDH|    replace:: :abbr:`CDH (Cloudera Hadoop)`
.. |COPY|   unicode:: U+000A9 .. Copyright symbol
.. |CSV|    replace:: :abbr:`CSV (Character-Separated Variables)`
.. |DEPRECATED|   image:: _static/deprecated.*
              :target: glossary.html#term-api-maturity-tags
.. |DNS|    replace:: :abbr:`DNS (Domain Name Service)`
.. |ECDF|   replace:: :abbr:`ECDF (Empirical Cumulative Distribution Function)`
.. |EM|     unicode:: U+02014 .. Long dash
.. |ETL|    replace:: :abbr:`ETL (extract, transform, and load)`
.. |HDFS|   replace:: :abbr:`HDFS (Hadoop Distributed File System)`
.. |IA|     replace:: Intel Analytics
.. |IAT|    replace:: :abbr:`ATK (Analytics Toolkit)`
.. |JSON|   replace:: :abbr:`JSON (JavaScript Object Notation)`
.. |K-S|    replace:: :abbr:`K-S (Kolmogorov-Smirnov)`
.. |LDA|    replace:: :abbr:`LDA (Latent Dirichlet Allocation)`
.. |LSI|    replace:: :abbr:`LSI (Latent Semantic Indexing)1
.. |MDA|    replace:: :abbr:`MDA (Multi-Dimensional Analytical)`
.. |OLAP|   replace:: :abbr:`OLAP (OnLine Analytical Processing)`
.. |OLTP|   replace:: :abbr:`OLAP (OnLine Transaction Processing)`
.. |RTM|    unicode:: U+000AE .. Registered Trade Mark symbol
.. |TRADE|  unicode:: U+2122 .. Trademark symbol
.. |XML|    replace:: :abbr:`XML (Extensible Markup Language)`
.. |YCSB|   replace:: :abbr:`YCSB (Yahoo! Cloud Serving Benchmarking)`
"""

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#sys.path.insert(0, os.path.abspath('.'))
p = os.path.abspath('../../python/')
print "Adding path " + p
sys.path.insert(0, p)# os.path.abspath('../../..'))
print "sys.path is now:"
print "\n".join(sys.path)

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
#    'sphinx.ext.intersphinx',
    'sphinx.ext.ifconfig',
    'sphinx.ext.autosummary',
    'sphinx.ext.todo',
    'sphinx.ext.doctest',
    'sphinx.ext.pngmath',
#    'sphinx.ext.viewcode',
    'numpydoc',
]

# This is a flag to print out To-Do items
todo_include_todos = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The encoding of source files.
#source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'Intel Analytics'
#project = 'Welcome to the Intel Big Data Platform: Analytics Toolkit'
copyright = u'2015, Intel - INTEL CONFIDENTIAL'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = '1.0'
# The full version, including alpha/beta/rc tags.
release = '1.0.0'

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#language = None

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#today = ''
# Else, today_fmt is used as the format for a strftime call.
#today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = []

# The reST default role (used for this markup: `text`) to use for all
# documents.
#default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
#show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
#modindex_common_prefix = []
html_use_modindex = True

# If true, keep warnings as "system message" paragraphs in the built documents.
#keep_warnings = False

# The following two functions cause Sphinx to document the __init__ function, which it normally skips
#def skip(app, what, name, obj, skip, options):
#    if name == "__init__":
#        return False
#    return skip
#
def setup(app):
#    app.connect("autodoc-skip-member", skip)
    app.connect('autodoc-skip-member', autodoc_skip_member)

autodoc_default_flags = ['members']

def autodoc_skip_member(app, what, name, obj, skip, options):
    exclusions = ('Rule')
    exclude = name in exclusions
    return skip or exclude

# -- Options for HTML output ----------------------------------------------

# html_style = 'strike.css'

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#html_theme = 'default'
#html_theme = 'nature'
#html_theme = 'agogo'
#html_theme = 'scipy'
#html_theme = 'basic'
#html_theme = 'epub'
#html_theme = 'haiku'
#html_theme = 'pyramid'
#html_theme = 'scrolls'
#html_theme = 'sphinxdoc'
#html_theme = 'traditional'
html_theme = 'IA'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#html_theme_options = {"stickysidebar" : "True"}

# Add any paths that contain custom themes here, relative to this directory.
html_theme_path = ['_theme']

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
#html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
html_short_title = "Intel Analytics"

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
html_logo = "_static/intel-logo-small.jpg"

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
#html_favicon = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
#html_extra_path = []

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
# html_sidebars = {'**': ['searchbox.html', 'globaltoc.html']}
html_sidebars = {'**': ['globaltoc.html']}

# Additional templates that should be rendered to pages, maps page names to
# template names.
#html_additional_pages = {}

# If false, no module index is generated.
html_domain_indices = True

# If false, no index is generated.
html_use_index = True

# If true, the index is split into individual pages for each letter.
#html_split_index = False

# If true, links to the reST sources are added to the pages.
#html_show_sourcelink = False

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
#html_file_suffix = None

# Output file base name for HTML help builder.
#htmlhelp_basename = 'IntelAnalyticsDoc'


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
# The paper size ('letterpaper' or 'a4paper').
'papersize': 'letterpaper',
'printindex': '\\printindex',

# The font size ('10pt', '11pt' or '12pt').
'pointsize': '10pt',

# Additional stuff for the LaTeX preamble.
'preamble': '\\usepackage{mathtools}\n\\everymath{\\displaystyle}\n\\raggedright'
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    ('index', 'IntelAnalytics.tex', u'Intel Analytics Documentation', u'Intel', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
latex_logo = "_static/intel-logo.jpg"

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
latex_use_parts = True

# If true, show page references after internal links.
latex_show_pagerefs = False

# If true, show URL addresses after external links.
latex_show_urls = 'footnote'

# Documents to append as an appendix to all manuals.
#latex_appendices = []

# If false, no module index is generated.
latex_domain_indices = True


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    ('index', 'pythonapi', u'IntelAnalytics Documentation',
     [u'Intel'], 1)
]

# If true, show URL addresses after external links.
#man_show_urls = False


# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
  ('index', 'IntelAnalytics', u'IntelAnalytics Documentation',
   u'Intel', 'IntelAnalytics', 'One line description of project.',
   'Miscellaneous'),
]

# Documents to append as an appendix to all manuals.
#texinfo_appendices = []

# If false, no module index is generated.
#texinfo_domain_indices = True

# How to display URL addresses: 'footnote', 'no', or 'inline'.
#texinfo_show_urls = 'footnote'

# If true, do not generate a @detailmenu in the "Top" node's menu.
#texinfo_no_detailmenu = False

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {'http://docs.python.org/': None}
