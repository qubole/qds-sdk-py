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
        sys.argv = ['qds.py', 'qbucket_subscriber', 'create', '--qbucket_id', '1', '--cross_account_config_id', '1']
        print_command()
        Connection._api_call = Mock(return_value={"qbucket_subscriber": []})
        qds.main()
        Connection._api_call.assert_called_with("POST", "qbucket_subscribers", {'qbucket_id': '1', 'cross_account_config_id': '1'})

    def test_list(self):
        sys.argv = ['qds.py', 'qbucket_subscriber', 'list']
        print_command()
        Connection._api_call = Mock(return_value={"qbucket_subscriber": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "qbucket_subscribers", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'qbucket_subscriber', 'view', '1']
        print_command()
        Connection._api_call = Mock(return_value={"qbucket_subscriber": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "qbucket_subscribers/1", params=None)

    # def test_edit(self):
    #     sys.argv = ['qds.py', 'qbucket_subscriber', 'edit', '1', '--storage_access_key', '000000000000000000', '--storage_secret_key', '00000000000000000000']
    #     print_command()
    #     Connection._api_call = Mock(return_value={"qbucket_subscriber": []})
    #     qds.main()
    #     Connection._api_call.assert_called_with("PUT", "qbucket_subscribers/1", {'storage_access_key': '000000000000000000', 'storage_secret_key': '00000000000000000000'})

    # def test_delete(self):
    #     sys.argv = ['qds.py', 'qbucket_subscriber', 'delete', '1']
    #     print_command()
    #     Connection._api_call = Mock(return_value={"qbucket_subscriber": []})
    #     qds.main()
    #     Connection._api_call.assert_called_with("DELETE", "qbucket_subscribers/1", params=None)

if __name__ == '__main__':
    unittest.main()