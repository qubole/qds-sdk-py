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


class TestSpace(QdsCliTestCase):

    def test_create(self):
        sys.argv = ['qds.py', 'space', 'create', '--name', 'test_space', '--uri', 's3://dev.canopydata.com/unittest', '--acl', 'private']
        print_command()
        Connection._api_call = Mock(return_value={"spaces":[]})
        qds.main()
        Connection._api_call.assert_called_with("POST", "spaces", {'name': 'test_space', 'uri': 's3://dev.canopydata.com/unittest', 'acl': 'private'})

    def test_list(self):
        sys.argv = ['qds.py', 'space', 'list']
        print_command()
        Connection._api_call = Mock(return_value={"spaces":[]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "spaces", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'space', 'view', '1']
        print_command()
        Connection._api_call = Mock(return_value={"spaces":[]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "spaces/1", params=None)

    def test_hivetables(self):
        sys.argv = ['qds.py', 'space', 'hivetables', '1']
        print_command()
        Connection._api_call = Mock(return_value={"spaces":[]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "spaces/1/hivetables", params=None)

if __name__ == '__main__':
    unittest.main()