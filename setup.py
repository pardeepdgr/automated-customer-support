#!/usr/bin/env python

from setuptools import setup

setup(
   name='automated-customer-support',
   version='1.0',
   description='automated-customer-support',
   author='Pardeep Dogra',
   author_email='pardeepdgr@gmail.com',
   packages=['automated-customer-support'],
   install_requires=[
       'beautifulsoup4',
       'pyspark',
       'requests'
   ]
)
