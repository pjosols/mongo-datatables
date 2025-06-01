from setuptools import setup, find_packages
import os

# Read the contents of your README file
this_directory = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
    long_description_content_type = 'text/markdown'
except FileNotFoundError:
    # Fallback to README.rst if README.md doesn't exist
    with open(os.path.join(this_directory, 'README.rst'), encoding='utf-8') as f:
        long_description = f.read()
    long_description_content_type = 'text/x-rst'

setup(
    name='mongo_datatables',
    version='1.1.2',
    description='Server-side processing for DataTables and Editor with MongoDB',
    long_description=long_description,
    long_description_content_type=long_description_content_type,
    url='https://github.com/pjosols/mongo-datatables',
    author='Paul Olsen',
    author_email='pjosols@wholeshoot.com',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'pymongo>=3.9.0',
    ],
    python_requires='>=3.8',
    extras_require={
        'dev': [
            'pytest',
            'pytest-cov',
            'coverage',
            'tox',
            'sphinx',
            'sphinx_rtd_theme',
        ],
        'test': [
            'pytest',
            'pytest-cov',
            'coverage',
        ],
        'docs': [
            'sphinx',
            'sphinx-rtd-theme'
        ],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Framework :: Flask',
        'Framework :: Django',
    ],
    keywords='datatables editor mongodb pymongo flask django server-side',
    project_urls={
        'Bug Reports': 'https://github.com/pjosols/mongo-datatables/issues',
        'Source': 'https://github.com/pjosols/mongo-datatables',
        'Documentation': 'https://github.com/pjosols/mongo-datatables#readme',
    },
    include_package_data=True,
    zip_safe=False
)