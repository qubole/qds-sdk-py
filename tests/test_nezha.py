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


class TestNezhaDataSource(QdsCliTestCase):
    def test_list(self):
        sys.argv = ['qds.py', 'nezha', 'data_sources', 'list']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "nezha_data_sources", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'nezha', 'data_sources', 'view', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_data_sources/123", params=None)

    def test_view_neg(self):
        sys.argv = ['qds.py', 'nezha', 'data_sources', 'view']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_update_url(self):
        sys.argv = ['qds.py', 'nezha', 'data_sources', 'update', '123', '--url', 'http://test']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_data_sources/123", {'url': 'http://test'})

    def test_update_two_fields(self):
        sys.argv = ['qds.py', 'nezha', 'data_sources', 'update', '123', '--url', 'http://test', '--name', 'test']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_data_sources/123", {'name': 'test', 'url': 'http://test'})

    def test_delete(self):
        sys.argv = ['qds.py', 'nezha', 'data_sources', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("DELETE", "nezha_data_sources/123", None)

    def test_delete_neg(self):
        sys.argv = ['qds.py', 'nezha', 'data_sources', 'delete']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

class TestNezhaCube(QdsCliTestCase):
    def test_list(self):
        sys.argv = ['qds.py', 'nezha', 'cubes', 'list']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_cubes': []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "nezha_cubes", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'nezha', 'cubes', 'view', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_cubes/123", params=None)

    def test_view_neg(self):
        sys.argv = ['qds.py', 'nezha', 'cubes', 'view']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_update_schema_name(self):
        sys.argv = ['qds.py', 'nezha', 'cubes', 'update', '123', '--schema_name', 'something']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_cubes': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_cubes/123", {'schema_name': 'something'})

    def test_update_two_fields(self):
        sys.argv = ['qds.py', 'nezha', 'cubes', 'update', '123', '--cost', '100', '--query', 'select now()']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_cubes': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_cubes/123", {'cost': '100', 'query': 'select now()'})

    def test_delete(self):
        sys.argv = ['qds.py', 'nezha', 'cubes', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("DELETE", "nezha_cubes/123", None)

    def test_delete_neg(self):
        sys.argv = ['qds.py', 'nezha', 'cubes', 'delete']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

class TestNezhaPartition(QdsCliTestCase):
    def test_list(self):
        sys.argv = ['qds.py', 'nezha', 'partitions', 'list']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_partitions': []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "nezha_partitions", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'nezha', 'partitions', 'view', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_partitions/123", params=None)

    def test_view_neg(self):
        sys.argv = ['qds.py', 'nezha', 'partitions', 'view']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_update_schema_name(self):
        sys.argv = ['qds.py', 'nezha', 'partitions', 'update', '123', '--schema_name', 'something']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_partitions': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_partitions/123", {'schema_name': 'something'})

    def test_update_two_fields(self):
        sys.argv = ['qds.py', 'nezha', 'partitions', 'update', '123', '--cost', '100', '--query', 'select now()']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_partitions': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_partitions/123", {'cost': '100', 'query': 'select now()'})

    def test_delete(self):
        sys.argv = ['qds.py', 'nezha', 'partitions', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("DELETE", "nezha_partitions/123", None)

    def test_delete_neg(self):
        sys.argv = ['qds.py', 'nezha', 'partitions', 'delete']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

if __name__ == '__main__':
    unittest.main()
