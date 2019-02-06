"""Defines how metakb is packaged and distributed."""
from setuptools import setup

setup(name='gen3_etl',
      version='0.0.0',
      description='Transform bcc to gen3',
      url='https://github.com/ohsu-comp-bio/bcc',
      author='walsbr',
      author_email='walsbr@gohsu.edu',
      license='MIT',
      packages=['gen3_etl'],
      zip_safe=False)
