from __future__ import print_function
import sys
import os
if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import *
import tempfile
sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
import qds_sdk
from qds_sdk.connection import Connection
from qds_sdk.resource import BaseResource
from test_base import print_command
from test_base import QdsCliTestCase


class TestQbucket(QdsCliTestCase):

    def test_create(self):
        sys.argv = ['qds.py', 'qbucket', 'create', '--name', 'test_qbucket', '--path', 's3://dev.canopydata.com/unittest', '--acl', 'private', '--object_store_type', 's3']
        print_command()
        Connection._api_call = Mock(return_value={"qbuckets":[]})
        qds.main()
        Connection._api_call.assert_called_with("POST", "qbuckets", {'name': 'test_qbucket', 'path': 's3://dev.canopydata.com/unittest', 'acl': 'private', 'object_store_type': 's3'})

    def test_list(self):
        sys.argv = ['qds.py', 'qbucket', 'list']
        print_command()
        Connection._api_call = Mock(return_value={"qbuckets":[]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "qbuckets", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'qbucket', 'view', '1']
        print_command()
        Connection._api_call = Mock(return_value={"qbuckets":[]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "qbuckets/1", params=None)

if __name__ == '__main__':
    unittest.main()