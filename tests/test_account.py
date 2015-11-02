from __future__ import print_function
import sys
import os
if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import Mock
import tempfile
sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase


class TestAccountCreate(QdsCliTestCase):

    def test_all(self):
        sys.argv = ['qds.py', 'account', 'create', '--name', 'new_account', '--location', 's3://dev.canopydata.com' ,
                    '--storage-access-key', 'dummy',  '--storage-secret-key', 'dummy', '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy', '--previous-account-plan', 'true', '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'name': 'new_account',
            'acc_key': 'dummy',
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': 'us-east-1', 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': 'dummy',
            'use_previous_account_plan': 'true',
            'compute_secret_key': 'dummy',
            'compute_access_key': 'dummy', 'defloc': 's3://dev.canopydata.com'}})

    def test_invalid(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--level', 'invalid', '--name', 'new_account', '--location', 's3://dev.canopydata.com' ,
                    '--storage-access-key', 'dummy',  '--storage-secret-key', 'dummy', '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy', '--previous-account-plan', 'true', '--aws-region', 'us-east-1' ]
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_name(self):
        sys.argv = ['qds.py', 'account', 'create', '--name', 'new_account']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_storage_acc_key(self):
        sys.argv = ['qds.py', 'account', 'create', '--storage-access-key', 'dummy']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_storage_secret_key(self):
        sys.argv = ['qds.py', 'account', 'create', '--storage-secret-key', 'dummy']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_aws_region(self):
        sys.argv = ['qds.py', 'account', 'create', '--aws-region', 'us-east-1' ]
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_compute_acc_key(self):
        sys.argv = ['qds.py', 'account', 'create', '--compute-access-key', 'dummy']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_compute_secret_key(self):
        sys.argv = ['qds.py', 'account', 'create', '--compute-secret-key', 'dummy']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_location(self):
        sys.argv = ['qds.py', 'account', 'create', '--location', 's3://dev.canopydata.com']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_previous_account_plan(self):
        sys.argv = ['qds.py', 'account', 'create', '--previous-account-plan', 'true']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

if __name__ == '__main__':
    unittest.main()
