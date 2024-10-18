# -*- coding: utf-8 -*-
#
# pyspark documentation build configuration file, created by
# sphinx-quickstart on Thu Aug 28 15:17:47 2014.
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
import shutil
import errno

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath('.'))

# generate user_guide/pandas_on_spark/supported_pandas_api.rst
from pyspark.pandas.supported_api_gen import generate_supported_api

output_rst_file_path = (
    "%s/user_guide/pandas_on_spark/supported_pandas_api.rst"
    % os.path.dirname(os.path.abspath(__file__))
)
generate_supported_api(output_rst_file_path)

# Remove previously generated rst files. Ignore errors just in case it stops
# generating whole docs.
shutil.rmtree(
    "%s/reference/api" % os.path.dirname(os.path.abspath(__file__)), ignore_errors=True)
shutil.rmtree(
    "%s/reference/pyspark.pandas/api" % os.path.dirname(os.path.abspath(__file__)),
    ignore_errors=True)
try:
    os.mkdir("%s/reference/api" % os.path.dirname(os.path.abspath(__file__)))
except OSError as e:
    if e.errno != errno.EEXIST:
        raise
try:
    os.mkdir("%s/reference/pyspark.pandas/api" % os.path.dirname(
        os.path.abspath(__file__)))
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
needs_sphinx = '1.2'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.mathjax',
    'sphinx.ext.autosummary',
    'sphinx_copybutton',
    'nbsphinx',  # Converts Jupyter Notebook to reStructuredText files for Sphinx.
    # For ipython directive in reStructuredText files. It is generated by the notebook.
    'IPython.sphinxext.ipython_console_highlighting',
    'numpydoc',  # handle NumPy documentation formatted docstrings.
    'sphinx_plotly_directive',  # For visualize plot result
]

# sphinx copy button
copybutton_exclude = '.linenos, .gp, .go'

# plotly plot directive
plotly_include_source = True
plotly_html_show_formats = False
plotly_html_show_source_link = False
plotly_pre_code = """import numpy as np
import pandas as pd
import pyspark.pandas as ps"""

numpydoc_show_class_members = False

# Links used globally in the RST files.
# These are defined here to allow link substitutions dynamically.
rst_epilog = """
.. |binder| replace:: Live Notebook
.. _binder: https://mybinder.org/v2/gh/apache/spark/{0}?filepath=python%2Fdocs%2Fsource%2Fgetting_started%2Fquickstart_df.ipynb
.. |binder_df| replace:: Live Notebook: DataFrame
.. _binder_df: https://mybinder.org/v2/gh/apache/spark/{0}?filepath=python%2Fdocs%2Fsource%2Fgetting_started%2Fquickstart_df.ipynb
.. |binder_ps| replace:: Live Notebook: pandas API on Spark
.. _binder_ps: https://mybinder.org/v2/gh/apache/spark/{0}?filepath=python%2Fdocs%2Fsource%2Fgetting_started%2Fquickstart_ps.ipynb
.. |binder_connect| replace:: Live Notebook: Spark Connect
.. _binder_connect: https://mybinder.org/v2/gh/apache/spark/{0}?filepath=python%2Fdocs%2Fsource%2Fgetting_started%2Fquickstart_connect.ipynb
.. |examples| replace:: Examples
.. _examples: https://github.com/apache/spark/tree/{0}/examples/src/main/python
.. |downloading| replace:: Downloading
.. _downloading: https://spark.apache.org/docs/{1}/#downloading
.. |building_spark| replace:: Building Spark
.. _building_spark: https://spark.apache.org/docs/{1}/building-spark.html
""".format(
    os.environ.get("GIT_HASH", "master"),
    os.environ.get("RELEASE_VERSION", "latest"),
)

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The encoding of source files.
#source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'PySpark'
copyright = ''

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = 'master'
# The full version, including alpha/beta/rc tags.
release = os.environ.get('RELEASE_VERSION', version)

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
exclude_patterns = ['_build', '.DS_Store', '**.ipynb_checkpoints']

# The reST default role (used for this markup: `text`) to use for all
# documents.
#default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
#add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
#show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
#modindex_common_prefix = []

# If true, keep warnings as "system message" paragraphs in the built documents.
#keep_warnings = False

# -- Options for autodoc --------------------------------------------------

# Look at the first line of the docstring for function and method signatures.
autodoc_docstring_signature = True
autosummary_generate = True

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'pydata_sphinx_theme'

html_context = {
    # When releasing a new Spark version, please update the file
    # "site/static/versions.json" under the code repository "spark-website"
    # (item should be added in order), and also set the local environment
    # variable "RELEASE_VERSION".
    "switcher_json_url": "https://spark.apache.org/static/versions.json",
    "switcher_template_url": "https://spark.apache.org/docs/{version}/api/python/index.html",
}

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    "navbar_end": ["version-switcher"]
}

# Add any paths that contain custom themes here, relative to this directory.
#html_theme_path = []

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
#html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
#html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
html_logo = "../../../docs/img/spark-logo-reverse.png"

# The name of an image file (within the static path) to use as a favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
#html_favicon = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_css_files = [
    'css/pyspark.css',
]

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
#html_extra_path = []

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
#html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
#html_sidebars = {}

# Additional templates that should be rendered to pages; maps page names to
# template names.
#html_additional_pages = {}

# If false, no module index is generated.
html_domain_indices = False

# If false, no index is generated.
html_use_index = False

# If true, the index is split into individual pages for each letter.
#html_split_index = False

# If true, links to the reST sources are added to the pages.
#html_show_sourcelink = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
#html_show_sphinx = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
#html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
#html_file_suffix = None

# Output file base name for HTML help builder.
htmlhelp_basename = 'pysparkdoc'

# The base URL which points to the root of the HTML documentation.
html_baseurl = 'https://spark.apache.org/docs/latest/api/python'

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
# The paper size ('letterpaper' or 'a4paper').
#'papersize': 'letterpaper',

# The font size ('10pt', '11pt' or '12pt').
#'pointsize': '10pt',

# Additional stuff for the LaTeX preamble.
#'preamble': '',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
  ('index', 'pyspark.tex', 'pyspark Documentation',
   'Author', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
#latex_use_parts = False

# If true, show page references after internal links.
#latex_show_pagerefs = False

# If true, show URL addresses after external links.
#latex_show_urls = False

# Documents to append as an appendix to all manuals.
#latex_appendices = []

# If false, no module index is generated.
#latex_domain_indices = True


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    ('index', 'pyspark', 'pyspark Documentation',
     ['Author'], 1)
]

# If true, show URL addresses after external links.
#man_show_urls = False


# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
  ('index', 'pyspark', 'pyspark Documentation',
   'Author', 'pyspark', 'One line description of project.',
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


# -- Options for Epub output ----------------------------------------------

# Bibliographic Dublin Core info.
epub_title = 'pyspark'
epub_author = 'Author'
epub_publisher = 'Author'
epub_copyright = '2014, Author'

# The basename for the epub file. It defaults to the project name.
#epub_basename = 'pyspark'

# The HTML theme for the epub output. Since the default themes are not optimized
# for small screen space, using the same theme for HTML and epub output is
# usually not wise. This defaults to 'epub', a theme designed to save visual
# space.
#epub_theme = 'epub'

# The language of the text. It defaults to the language option
# or en if the language is not set.
#epub_language = ''

# The scheme of the identifier. Typical schemes are ISBN or URL.
#epub_scheme = ''

# The unique identifier of the text. This can be an ISBN number
# or the project homepage.
#epub_identifier = ''

# A unique identification for the text.
#epub_uid = ''

# A tuple containing the cover image and cover page html template filenames.
#epub_cover = ()

# A sequence of (type, uri, title) tuples for the guide element of content.opf.
#epub_guide = ()

# HTML files that should be inserted before the pages created by sphinx.
# The format is a list of tuples containing the path and title.
#epub_pre_files = []

# HTML files that should be inserted after the pages created by sphinx.
# The format is a list of tuples containing the path and title.
#epub_post_files = []

# A list of files that should not be packed into the epub file.
epub_exclude_files = ['search.html']

# The depth of the table of contents in toc.ncx.
#epub_tocdepth = 3

# Allow duplicate toc entries.
#epub_tocdup = True

# Choose between 'default' and 'includehidden'.
#epub_tocscope = 'default'

# Fix unsupported image types using the PIL.
#epub_fix_images = False

# Scale large images.
#epub_max_image_width = 0

# How to display URL addresses: 'footnote', 'no', or 'inline'.
#epub_show_urls = 'inline'

# If false, no index is generated.
#epub_use_index = True

# Skip sample endpoint link (not expected to resolve)
linkcheck_ignore = [r'https://kinesis.us-east-1.amazonaws.com']
