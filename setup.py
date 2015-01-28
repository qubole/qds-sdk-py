import os
import sys
from setuptools import setup

INSTALL_REQUIRES = ['requests >=1.0.3', 'boto >=2.1.1', 'six >=1.2.0']
if sys.version_info < (2, 7, 0):
    INSTALL_REQUIRES.append('argparse>=1.1')

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "qds_sdk",
    version = "1.3.1",
    author = "Qubole",
    author_email = "dev@qubole.com",
    description = ("Python SDK for coding to the Qubole Data Service API"),
    keywords = "qubole sdk api",
    url = "https://github.com/qubole/qds-sdk-py",
    packages=['qds_sdk'],
    scripts=['bin/qds.py'],
    install_requires=INSTALL_REQUIRES,
    long_description=read('README.rst'),
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4"
    ]
    )
