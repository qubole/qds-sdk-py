import os
import sys
from setuptools import setup
from subprocess import call

INSTALL_REQUIRES = ['requests >=1.0.3', 'boto >=2.1.1', 'six >=1.2.0', 'urllib3 >= 1.0.2', 'inflection >= 0.3.1']

# This is just a temporary change as the python SDK is not public in PyPI so using subprocess to download and install it
def install_oracle_bmc_sdk():
    try:
        import oraclebmc
    except ImportError:
        try:
            if sys.version_info == (2,7,5) or sys.version_info >= (3,5,0):
                call(["wget",
                      "https://docs.us-az-phoenix-1.oracleiaas.com/tools/python/latest/download/oracle-bmcs-python-sdk.zip"])
                call(["unzip", "oracle-bmcs-python-sdk.zip"])
                call(["pip", "install", "oracle-bmcs-python-sdk/oraclebmc-1.0.0-py2.py3-none-any.whl"])
                call(["rm", "-rf","oracle-bmcs-python-sdk.zip", "oracle-bmcs-python-sdk"])
        except:
            print("Error in installing oracle bmc sdk")


install_oracle_bmc_sdk()

if sys.version_info < (2, 7, 0):
    INSTALL_REQUIRES.append('argparse>=1.1')

if sys.version_info >= (2,7,0):
    INSTALL_REQUIRES.append('azure==2.0.0rc6')


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="qds_sdk",
    version="unreleased",
    author="Qubole",
    author_email="dev@qubole.com",
    description=("Python SDK for coding to the Qubole Data Service API"),
    keywords="qubole sdk api",
    url="https://github.com/qubole/qds-sdk-py",
    packages=['qds_sdk'],
    scripts=['bin/qds.py'],
    install_requires=INSTALL_REQUIRES,
    long_description=read('README.rst'),
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4"
    ]
    )
