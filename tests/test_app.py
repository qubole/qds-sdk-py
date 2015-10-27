from __future__ import print_function
import sys
import os

if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import Mock

sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase


class TestAppList(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'app', 'list']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "apps", params=None)


class TestAppShow(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'app', 'show', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "apps/123", params=None)

    def test_fail_with_no_id(self):
        sys.argv = ['qds.py', 'app', 'show']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fail_with_bad_id(self):
        sys.argv = ['qds.py', 'app', 'show', 'notanumber']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestAppCreate(QdsCliTestCase):
    def test_fail_with_no_argument(self):
        sys.argv = ['qds.py', 'app', 'create']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_minimal(self):
        sys.argv = ['qds.py', 'app', 'create', '--name', 'appname']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "apps",
                                                {'name': 'appname',
                                                 'kind': 'spark',
                                                 'config': {}})

    def test_with_kind(self):
        sys.argv = ['qds.py', 'app', 'create', '--name', 'appname',
                    '--kind', 'spark']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "apps",
                                                {'name': 'appname',
                                                 'kind': 'spark',
                                                 'config': {}})

    def test_fail_with_wrong_kind(self):
        sys.argv = ['qds.py', 'app', 'create', '--name', 'appname',
                    '--kind', 'tez']  # tez apps are not supported yet.
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_with_config(self):
        sys.argv = ['qds.py', 'app', 'create', '--name', 'appname', '--config',
                    'zeppelin.spark.concurrentSQL=true']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "POST", "apps", {'name': 'appname', 'kind': 'spark',
                             'config': {
                                 'zeppelin.spark.concurrentSQL': 'true'}})

    def test_with_configs(self):
        sys.argv = ['qds.py', 'app', 'create', '--name', 'appname', '--config',
                    'spark.executor.memory=2g',
                    'zeppelin.spark.concurrentSQL=true']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "POST", "apps", {'name': 'appname', 'kind': 'spark',
                             'config': {
                                 'spark.executor.memory': '2g',
                                 'zeppelin.spark.concurrentSQL': 'true'}})

    def test_fail_with_bad_config_1(self):
        sys.argv = ['qds.py', 'app', 'create', '--name', 'appname', '--config',
                    'no-equal-sign']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fail_with_bad_config_2(self):
        sys.argv = ['qds.py', 'app', 'create', '--name', 'appname', '--config',
                    'multiple=equal=sign']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fail_with_good_and_bad_config(self):
        sys.argv = ['qds.py', 'app', 'create', '--name', 'appname', '--config',
                    'this=good', 'no-equal-sign']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestAppStop(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'app', 'stop', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "apps/123/stop", None)

    def test_fail_with_no_id(self):
        sys.argv = ['qds.py', 'app', 'stop']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fail_with_bad_id(self):
        sys.argv = ['qds.py', 'app', 'stop', 'notanumber']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestAppDelete(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'app', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("DELETE", "apps/123", None)

    def test_fail_with_no_id(self):
        sys.argv = ['qds.py', 'app', 'delete']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fail_with_bad_id(self):
        sys.argv = ['qds.py', 'app', 'delete', 'notanumber']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


if __name__ == '__main__':
    unittest.main()
