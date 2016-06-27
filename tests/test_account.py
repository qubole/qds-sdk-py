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


class TestAccountCreate(QdsCliTestCase):
    def test_all(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--location', 's3://bucket/path',
                    '--storage-access-key', 'dummy',
                    '--storage-secret-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--previous-account-plan', 'true',
                    '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'name': 'new_account',
            'acc_key': 'dummy',
            'level': 'free',
            'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': 'us-east-1',
            'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25',
            'secret': 'dummy',
            'use_previous_account_plan': 'true',
            'compute_secret_key': 'dummy',
            'compute_access_key': 'dummy',
            'defloc': 's3://bucket/path'}})

    def test_no_name(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--location', 's3://bucket/path',
                    '--storage-access-key', 'dummy',
                    '--storage-secret-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--previous-account-plan', 'true',
                    '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_no_storage_acc_key(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--location', 's3://bucket/path',
                    '--storage-secret-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--previous-account-plan', 'true',
                    '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_no_storage_secret_key(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--location', 's3://bucket/path',
                    '--storage-access-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--previous-account-plan', 'true',
                    '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_no_aws_region(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--location', 's3://bucket/path',
                    '--storage-access-key', 'dummy',
                    '--storage-secret-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--previous-account-plan', 'true']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_invalid_region(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--location', 's3://bucket/path',
                    '--storage-access-key', 'dummy',
                    '--storage-secret-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--previous-account-plan', 'true',
                    '--aws-region', 'non-existent']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_no_compute_acc_key(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--location', 's3://bucket/path',
                    '--storage-access-key', 'dummy',
                    '--storage-secret-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--previous-account-plan', 'true',
                    '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_no_compute_secret_key(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--location', 's3://bucket/path',
                    '--storage-access-key', 'dummy',
                    '--storage-secret-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--previous-account-plan', 'true',
                    '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_no_location(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--storage-access-key', 'dummy',
                    '--storage-secret-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--previous-account-plan', 'true',
                    '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_default_previous_account_plan(self):
        sys.argv = ['qds.py', 'account', 'create',
                    '--name', 'new_account',
                    '--location', 's3://bucket/path',
                    '--storage-access-key', 'dummy',
                    '--storage-secret-key', 'dummy',
                    '--compute-access-key', 'dummy',
                    '--compute-secret-key', 'dummy',
                    '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account': {
            'name': 'new_account',
            'acc_key': 'dummy',
            'level': 'free',
            'compute_type': 'CUSTOMER_MANAGED',
            'aws_region': 'us-east-1',
            'storage_type': 'CUSTOMER_MANAGED',
            'CacheQuotaSizeInGB': '25',
            'secret': 'dummy',
            'use_previous_account_plan': 'false',
            'compute_secret_key': 'dummy',
            'compute_access_key': 'dummy',
            'defloc': 's3://bucket/path'}})


class TestAccountBranding(QdsCliTestCase):
    def test_logo(self):
        sys.argv = ['qds.py', 'account', 'branding',
                    '--account-id', '4',
                    '--logo-uri', 'https://www.xyz.com/image.jpg']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "accounts/branding", {'logo': {
            'logo_uri' : 'https://www.xyz.com/image.jpg'},
            'account_id' : '4'})

    def test_link(self):
        sys.argv = ['qds.py', 'account', 'branding',
                    '--account-id', '4',
                    '--link-url', 'https://www.xyz.com',
                    '--link-label', 'Documentation']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "accounts/branding", {'link': {
            'link_url' : 'https://www.xyz.com',
            'link_label' : 'Documentation'},
            'account_id' : '4'})

    def test_logo_link(self):
        sys.argv = ['qds.py', 'account', 'branding',
                    '--account-id', '4',
                    '--logo-uri', 'https://www.xyz.com/image.jpg',
                    '--link-url', 'https://www.xyz.com',
                    '--link-label', 'Documentation']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "accounts/branding", {'logo': {
            'logo_uri' : 'https://www.xyz.com/image.jpg'},
            'link': {'link_url' : 'https://www.xyz.com',
            'link_label' : 'Documentation'},
            'account_id' : '4'})

    def test_without_account_id(self):
        sys.argv = ['qds.py', 'account', 'branding',
                    '--logo-uri', 'https://www.xyz.com/image.jpg',
                    '--link-url', 'https://www.xyz.com',
                    '--link-label', 'Documentation']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()


if __name__ == '__main__':
    unittest.main()
