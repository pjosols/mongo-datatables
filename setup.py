from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='mongo_datatables',
      version='0.2.2',
      description='Classes for connecting DataTables and Editor to MongoDB',
      long_description=readme(),
      url='http://github.com/wholeshoot/mongo_datatables',
      author='Paul Olsen',
      author_email='python@wholeshoot.com',
      license='MIT',
      packages=['mongo_datatables'],
      install_requires=['pymongo'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Topic :: Database :: Database Engines/Servers',
      ],
      keywords='flask pymongo mongodb',
      include_package_data=True,
      zip_safe=False
      )
