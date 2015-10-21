from __future__ import print_function
import sys
import os
if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import *
sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase

class TestSpaceSubscriber(QdsCliTestCase):

    def test_create(self):
        sys.argv = ['qds.py', 'space_subscriber', 'create', '--space_id', '1', '--role_arn', 'xxxxxxxx', '--external_id', 'xxxxxx']
        print_command()
        Connection._api_call = Mock(return_value={"space_subscriber": []})
        qds.main()
        Connection._api_call.assert_called_with("POST", "space_subscribers", {'space_id': '1', 'role_arn': 'xxxxxxxx', 'external_id': 'xxxxxx'})

    def test_list(self):
        sys.argv = ['qds.py', 'space_subscriber', 'list']
        print_command()
        Connection._api_call = Mock(return_value={"space_subscriber": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "space_subscribers", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'space_subscriber', 'view', '1']
        print_command()
        Connection._api_call = Mock(return_value={"space_subscriber": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "space_subscribers/1", params=None)

    # def test_edit(self):
    #     sys.argv = ['qds.py', 'space_subscriber', 'edit', '1', '--role_arn', 'xxxxxxxx', '--external_id', 'xxxxxx']
    #     print_command()
    #     Connection._api_call = Mock(return_value={"space_subscriber": []})
    #     qds.main()
    #     Connection._api_call.assert_called_with("PUT", "space_subscribers/1", {'role_arn': 'xxxxxxxx', 'external_id': 'xxxxxx'})

    # def test_delete(self):
    #     sys.argv = ['qds.py', 'space_subscriber', 'delete', '1']
    #     print_command()
    #     Connection._api_call = Mock(return_value={"space_subscriber": []})
    #     qds.main()
    #     Connection._api_call.assert_called_with("DELETE", "space_subscribers/1", params=None)

    def test_hivetables(self):
        sys.argv = ['qds.py', 'space_subscriber', 'hivetables', '1']
        print_command()
        Connection._api_call = Mock(return_value={"space_subscriber": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "space_subscribers/1/hivetables", params=None)

if __name__ == '__main__':
    unittest.main()