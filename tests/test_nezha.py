from mock import *

import sys
import os
import qds

from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase

if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest

sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))


class TestNezhaDataSource(QdsCliTestCase):
    def test_list(self):
        sys.argv = ['qds.py', 'nezha', 'list', 'data_sources']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_data_sources", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'nezha', 'view', 'data_sources', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_data_sources/123", params=None)

    def test_view_neg(self):
        sys.argv = ['qds.py', 'nezha', 'view', 'data_sources']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_update_url(self):
        sys.argv = [
            'qds.py', 'nezha', 'update', 'data_sources', '123',
            '--url', 'http://test']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_data_sources/123", {'url': 'http://test'})

    def test_update_two_fields(self):
        sys.argv = [
            'qds.py', 'nezha', 'update', 'data_sources', '123',
            '--url', 'http://test', '--name', 'test']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_data_sources': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_data_sources/123",
            {'name': 'test', 'url': 'http://test'})

    def test_delete(self):
        sys.argv = ['qds.py', 'nezha', 'delete', 'data_sources', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "DELETE", "nezha_data_sources/123", None)

    def test_delete_neg(self):
        sys.argv = ['qds.py', 'nezha', 'delete', 'data_sources']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestNezhaCube(QdsCliTestCase):
    def test_list(self):
        sys.argv = ['qds.py', 'nezha', 'list', 'cubes']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_cubes': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_cubes", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'nezha', 'view', 'cubes', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_cubes/123", params=None)

    def test_view_with_fields(self):
        sys.argv = ['qds.py', 'nezha', 'view', 'cubes', '123',
                    '--fields', 'id', 'table_name']
        print_command()
        Connection._api_call = Mock(return_value={
            'name':'dummycubes', 'id':'1', 'table_name':'2'})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_cubes/123", params=None)

    def test_view_neg(self):
        sys.argv = ['qds.py', 'nezha', 'view', 'cubes']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_update_schema_name(self):
        sys.argv = [
            'qds.py', 'nezha', 'update', 'cubes', '123',
            '--schema_name', 'something']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_cubes': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_cubes/123", {'schema_name': 'something'})

    def test_update_two_fields(self):
        sys.argv = [
            'qds.py', 'nezha', 'update', 'cubes', '123',
            '--cost', '100', '--query', 'select now()']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_cubes': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_cubes/123",
            {'cost': '100', 'query': 'select now()'})

    def test_delete(self):
        sys.argv = ['qds.py', 'nezha', 'delete', 'cubes', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "DELETE", "nezha_cubes/123", None)

    def test_delete_neg(self):
        sys.argv = ['qds.py', 'nezha', 'delete', 'cubes']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestNezhaPartition(QdsCliTestCase):
    def test_list(self):
        sys.argv = ['qds.py', 'nezha', 'list', 'partitions']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_partitions': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_partitions", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'nezha', 'view', 'partitions', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "nezha_partitions/123", params=None)

    def test_view_neg(self):
        sys.argv = ['qds.py', 'nezha', 'view', 'partitions']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_update_schema_name(self):
        sys.argv = [
            'qds.py', 'nezha', 'update', 'partitions', '123',
            '--schema_name', 'something']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_partitions': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_partitions/123", {'schema_name': 'something'})

    def test_update_two_fields(self):
        sys.argv = [
            'qds.py', 'nezha', 'update', 'partitions', '123',
            '--cost', '100', '--query', 'select now()']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_partitions': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "nezha_partitions/123",
            {'cost': '100', 'query': 'select now()'})

    def test_delete(self):
        sys.argv = ['qds.py', 'nezha', 'delete', 'partitions', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "DELETE", "nezha_partitions/123", None)

    def test_delete_neg(self):
        sys.argv = ['qds.py', 'nezha', 'delete', 'partitions']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestNezhaDefaultDS(QdsCliTestCase):
    def test_list(self):
        sys.argv = ['qds.py', 'nezha', 'list', 'default_datasource']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_default_ds': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "accounts/default_datasource", params=None)

    def test_update_default_ds(self):
        sys.argv = ['qds.py', 'nezha', 'update', 'default_datasource', '123']
        print_command()
        Connection._api_call = Mock(return_value={'nezha_default_ds': []})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "accounts/default_datasource",
            {'default_datasource_id': '123'})


if __name__ == '__main__':
    unittest.main()
