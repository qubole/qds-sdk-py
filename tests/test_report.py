import sys
import os
import unittest2 as unittest
from mock import Mock
import tempfile
sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase


class TestReportList(QdsCliTestCase):

    def test_minimal(self):
        sys.argv = ['qds.py', 'report', 'list']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "reports", None)


class TestReportCanonicalHiveCommands(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands", {})

    def test_start_date(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--start-date', '2014-01-01']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands",
                {'start_date': '2014-01-01'})

    def test_end_date(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--end-date', '2014-01-01']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands",
                {'end_date': '2014-01-01'})

    def test_offset(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--offset', '10']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands",
                {'offset': 10})

    def test_limit(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--limit', '20']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands",
                {'limit': 20})

    def test_sort_frequency(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--sort', 'frequency']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands",
                {'sort_column': 'frequency'})

    def test_sort_cpu(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--sort', 'cpu']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands",
                {'sort_column': 'cpu'})

    def test_sort_fs_bytes_read(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--sort', 'fs_bytes_read']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands",
                {'sort_column': 'fs_bytes_read'})

    def test_sort_fs_bytes_written(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--sort', 'fs_bytes_written']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/canonical_hive_commands",
                {'sort_column': 'fs_bytes_written'})

    def test_sort_invalid(self):
        sys.argv = ['qds.py', 'report', 'canonical_hive_commands',
                '--sort', 'invalid']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestReportAllCommands(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'report', 'all_commands']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands", {})

    def test_start_date(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--start-date', '2014-01-01']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'start_date': '2014-01-01'})

    def test_end_date(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--end-date', '2014-01-01']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'end_date': '2014-01-01'})

    def test_offset(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--offset', '10']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'offset': 10})

    def test_limit(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--limit', '20']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'limit': 20})

    def test_sort_time(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--sort', 'time']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'sort_column': 'time'})

    def test_sort_cpu(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--sort', 'cpu']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'sort_column': 'cpu'})

    def test_sort_fs_bytes_read(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--sort', 'fs_bytes_read']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'sort_column': 'fs_bytes_read'})

    def test_sort_fs_bytes_written(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--sort', 'fs_bytes_written']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'sort_column': 'fs_bytes_written'})

    def test_sort_invalid(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--sort', 'invalid']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_by_user(self):
        sys.argv = ['qds.py', 'report', 'all_commands',
                '--by-user']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET",
                "reports/all_commands",
                {'by_user': True})


if __name__ == '__main__':
    unittest.main()
