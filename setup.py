#!/usr/bin/env python

from distutils.core import setup,Extension
setup(name='hdfs',
        version='1.0',
        description='Python Bindings for HDFS',
        author='Kien Trinh',
        author_email='kientt86@gmail.com',
        url='git@github.com:kientt86/libhdfs.git',
        ext_modules=[Extension(name='pyhdfs', sources=['pyhdfs.c'],  libraries=['hdfs','jvm'])])
