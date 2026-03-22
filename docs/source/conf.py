# Configuration file for the Sphinx documentation builder.

import os
import sys
sys.path.insert(0, os.path.abspath('../..'))

# -- Project information -----------------------------------------------------

project = 'mongo-datatables'
copyright = '2026 Wholeshoot'
author = 'Paul Olsen'

from mongo_datatables import __version__
release = __version__
html_title = 'documentation'

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
]

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

html_theme = 'furo'
html_static_path = ['_static']
html_css_files = ['custom.css']

html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#55402f",
        "color-brand-content": "#55402f",
        "color-admonition-background": "#f5f5f4",
        "color-link": "#57534e",
        "color-link--hover": "#1c1917",
        "color-link--visited": "#78716c",
        "color-foreground-primary": "#1c1917",
        "color-foreground-secondary": "#57534e",
        "color-background-primary": "#ffffff",
        "color-background-secondary": "#f5f5f4",
        "color-background-border": "#e6e3e1",
        "color-sidebar-background": "#f5f5f4",
        "color-sidebar-background-border": "#e6e3e1",
        "color-sidebar-brand-text": "#55402f",
        "color-sidebar-link-text--top-level": "#1c1917",
        "color-sidebar-link-text": "#57534e",
        "color-highlight-on-target": "#f5f5f4",
        "font-stack": "Inter, system-ui, -apple-system, sans-serif",
        "font-stack--monospace": "SF Mono, Fira Code, Consolas, monospace",
    },
    "dark_css_variables": {
        "color-brand-primary": "#dbc0ae",
        "color-brand-content": "#dbc0ae",
        "color-admonition-background": "#2c2926",
        "color-link": "#a8a29e",
        "color-link--hover": "#f5f5f4",
        "color-link--visited": "#78716c",
        "color-foreground-primary": "#f5f5f4",
        "color-foreground-secondary": "#a8a29e",
        "color-background-primary": "#1c1917",
        "color-background-secondary": "#211f1d",
        "color-background-border": "#33302d",
        "color-sidebar-background": "#211f1d",
        "color-sidebar-background-border": "#33302d",
        "color-sidebar-brand-text": "#dbc0ae",
        "color-sidebar-link-text--top-level": "#f5f5f4",
        "color-sidebar-link-text": "#a8a29e",
        "color-highlight-on-target": "#2c2926",
        "font-stack": "Inter, system-ui, -apple-system, sans-serif",
        "font-stack--monospace": "SF Mono, Fira Code, Consolas, monospace",
    },
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
    "top_of_page_buttons": ["view", "edit"],
    "source_repository": "https://github.com/pjosols/mongo-datatables",
    "source_branch": "main",
    "source_directory": "docs/source/",
    "announcement": (
        '<a href="https://mongo-datatables.com" style="color:inherit;text-decoration:none;">'
        'mongo-datatables <span style="color:#a8a29e;font-weight:600;font-size:0.8em;">v' + release + '</span></a>'
        '&nbsp;&nbsp;·&nbsp;&nbsp;'
        '<a href="https://mongo-datatables.com" style="color:inherit;text-decoration:underline;">Home</a>'
        '&nbsp;&nbsp;'
        '<a href="https://github.com/pjosols/mongo-datatables" style="color:inherit;text-decoration:underline;">GitHub</a>'
    ),
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/pjosols/mongo-datatables",
            "html": '<svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>',
            "class": "",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/mongo-datatables/",
            "html": '<svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 24 24"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"></path></svg>',
            "class": "",
        },
    ],
}

pygments_style = "sphinx"
pygments_dark_style = "monokai"

# -- Napoleon settings -------------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True
