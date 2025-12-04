# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
sys.path.insert(0, os.path.abspath('../dbtk'))  # Points to dbtk/ package


project = 'dbtk'
copyright = '2025, Scott Bailey'
author = 'Scott Bailey'
release = '0.8.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',      # Auto-generate docs from docstrings
    'sphinx.ext.napoleon',     # Support Google/NumPy style docstrings
    'sphinx.ext.viewcode',     # Add [source] links to code
    'sphinx.ext.intersphinx',  # Link to other project docs (like Python)
    'rst2pdf.pdfbuilder'       # generate PDF using `sphinx-build -b pdf source build/pdf`
    'sphinx_simplepdf'         # `make simplepdf`
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Autodoc configuration
autodoc_default_options = {
    'members': True,
    'undoc-members': False,
    'private-members': False,
    'special-members': '__init__',
    'inherited-members': False,
    'show-inheritance': True,
    'exclude-members': '__weakref__'
}

# Don't document imported members (like iskeyword)
autodoc_inherit_docstrings = False

# Move type hints to parameter descriptions instead of signature
autodoc_typehints = "description"

# Format for type hints in descriptions
autodoc_typehints_description_target = "documented"

# Simplify complex type annotations
python_use_unqualified_type_names = True

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
