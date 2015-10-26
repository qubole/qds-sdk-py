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

    def test_account_create(self):
        sys.argv = ['qds.py', 'account', 'create']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account", {'account':
                                            {'acc_key': None,
                                             'name': None,
                                             'level': 'free', 'compute_type': 'CUSTOMER_MANAGED',
                                             'aws_region': None, 'storage_type': 'CUSTOMER_MANAGED',
                                             'CacheQuotaSizeInGB': '25', 'secret': None,
                                             'use_previous_account_plan': None,
                                             'compute_secret_key': None,
                                             'compute_access_key': None, 'defloc': None}}
                                            )


    def test_account_create_invalid(self):
        sys.argv = ['qds.py', 'account', 'create',
                '--level', 'invalid']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

if __name__ == '__main__':
    unittest.main()

