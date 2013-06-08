import os
from setuptools import setup

INSTALL_REQUIRES = ['python_cjson', 'requests']

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "qds_sdk",
    version = "1.0.1",
    author = "Qubole",
    author_email = "dev@qubole.com",
    description = ("Python SDK for coding to the Qubole Data Service API"),
    keywords = "qubole sdk api",
    url = "http://packages.python.org/qds_sdk",
    packages=['qds_sdk'],
    scripts=['examples/qds.py'],
    install_requires=INSTALL_REQUIRES,
    long_description=read('README.md')
    )
