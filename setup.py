from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='mongo_datatables',
      version='0.3.0',
      description='Classes for connecting DataTables and Editor to MongoDB',
      long_description=readme(),
      url='https://github.com/pauljolsen/mongo-datatables',
      author='Paul Olsen',
      author_email='paul@wholeshoot.com',
      license='MIT',
      packages=['mongo_datatables'],
      install_requires=['pymongo'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Topic :: Database :: Database Engines/Servers',
      ],
      keywords='flask django pymongo mongodb',
      include_package_data=True,
      zip_safe=False
      )
