import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__), 'sqlalchemy_h2', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.rst')


setup(name='sqlalchemy_h2',
      version=VERSION,
      description="H2 Dialect for SQLAlchemy",
      long_description=open(readme).read(),
      classifiers=[
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: Implementation :: Jython',
      'Topic :: Database :: Front-Ends',
      ],
      keywords='H2 SQLAlchemy',
      author='adorsk',
      packages=['sqlalchemy_h2'],
      include_package_data=True,
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
              'h2 = sqlalchemy_h2.dialect',
         ]
      }
)
