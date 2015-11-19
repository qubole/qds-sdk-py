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


class TestNezhaCheck(QdsCliTestCase):
    def test_list(self):
        sys.argv = ['qds.py', 'nezha_data_sources', 'list']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "nezha_data_sources", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'nezha_data_sources', 'view', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_data_sources/123", params=None)

    def test_view_neg(self):
        sys.argv = ['qds.py', 'nezha_data_sources', 'view']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_update_url(self):
        sys.argv = ['qds.py', 'nezha_data_sources', 'update', '123', '--url', 'http://test']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_data_sources/123", {'url': 'http://test'})

    def test_update_two_fields(self):
        sys.argv = ['qds.py', 'nezha_data_sources', 'update', '123', '--url', 'http://test', '--name', 'test']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_data_sources/123", {'name': 'test', 'url': 'http://test'})

    def test_delete(self):
        sys.argv = ['qds.py', 'nezha_data_sources', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("DELETE", "nezha_data_sources/123", None)

    def test_delete_neg(self):
        sys.argv = ['qds.py', 'nezha_data_sources', 'delete']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

if __name__ == '__main__':
    unittest.main()
