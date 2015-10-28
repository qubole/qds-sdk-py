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

    def test_none(self):
        sys.argv = ['qds.py', 'account', 'create']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': None,
            'name': None,
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': None,
            'use_previous_account_plan': None,
            'compute_secret_key': None,
            'compute_access_key': None, 'defloc': None}})

    def test_invalid(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--level', 'invalid']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_name(self):
        sys.argv = ['qds.py', 'account', 'create', '--name', 'new_account']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': None,
            'name': "new_account",
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': None,
            'use_previous_account_plan': None,
            'compute_secret_key': None,
            'compute_access_key': None, 'defloc': None}})

    def test_storage_acc_key(self):
        sys.argv = ['qds.py', 'account', 'create', '--storage-access-key', 'dummy']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': "dummy",
            'name': None,
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': None,
            'use_previous_account_plan': None,
            'compute_secret_key': None,
            'compute_access_key': None, 'defloc': None}})

    def test_storage_secret_key(self):
        sys.argv = ['qds.py', 'account', 'create', '--storage-secret-key', 'dummy']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': None,
            'name': None,
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': "dummy",
            'use_previous_account_plan': None,
            'compute_secret_key': None,
            'compute_access_key': None, 'defloc': None}})

    def test_aws_region(self):
        sys.argv = ['qds.py', 'account', 'create', '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': None,
            'name': None,
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': 'us-east-1', 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': None,
            'use_previous_account_plan': None,
            'compute_secret_key': None,
            'compute_access_key': None, 'defloc': None}})

    def test_compute_acc_key(self):
        sys.argv = ['qds.py', 'account', 'create', '--compute-access-key', 'dummy']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': None,
            'name': None,
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': None,
            'use_previous_account_plan': None,
            'compute_secret_key': None,
            'compute_access_key': 'dummy', 'defloc': None}})

    def test_compute_secret_key(self):
        sys.argv = ['qds.py', 'account', 'create', '--compute-secret-key', 'dummy']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': None,
            'name': None,
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': None,
            'use_previous_account_plan': None,
            'compute_secret_key': "dummy",
            'compute_access_key': None, 'defloc': None}})

    def test_location(self):
        sys.argv = ['qds.py', 'account', 'create', '--location', 's3://dev.canopydata.com']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': None,
            'name': None,
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': None,
            'use_previous_account_plan': None,
            'compute_secret_key': None,
            'compute_access_key': None, 'defloc': 's3://dev.canopydata.com'}})

    def test_previous_account_plan(self):
        sys.argv = ['qds.py', 'account', 'create', '--previous-account-plan', 'true']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'acc_key': None,
            'name': None,
            'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25', 'secret': None,
            'use_previous_account_plan': 'true',
            'compute_secret_key': None,
            'compute_access_key': None, 'defloc': None}})

if __name__ == '__main__':
    unittest.main()
