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
import json


class TestAccount(QdsCliTestCase):
    def test_create(self):
        sys.argv = ['qds.py', 'account', 'create', '--account',
                    '{"name":"New_Account","level":"free","compute_type":"CUSTOMER_MANAGED",'
                    '"storage_type":"CUSTOMER_MANAGED","CacheQuotaSizeInGB":25,'
                    '"defloc":"dev.canopydata.com/unittest/122","acc_key":"xxxxxxxxx",'
                    '"secret":"xxxxxxxx","compute_access_key":"xxxxxxxx",'
                    '"compute_secret_key":"xxxxxxxx","aws_region":"us-east-1",'
                    '"use_previous_account_plan":"false"}']
        print_command()
        Connection._api_call = Mock(return_value={"account": []})
        qds.main()
        Connection._api_call.assert_called_with("POST", "account",
                                                {
                                                    'account': json.loads('{"name":"New_Account","level":"free",'
                                                                          '"compute_type":"CUSTOMER_MANAGED",'
                                                                          '"storage_type":"CUSTOMER_MANAGED",'
                                                                          '"CacheQuotaSizeInGB'
                                                                          '":25,'
                                                                          '"defloc":"dev.canopydata.com/unittest/122",'
                                                                          '"acc_key":"xxxxxxxxx",'
                                                                          '"secret":"xxxxxxxx",'
                                                                          '"compute_access_key":"xxxxxxxx",'
                                                                          '"compute_secret_key":"xxxxxxxx",'
                                                                          '"aws_region":"us-east-1",'
                                                                          '"use_previous_account_plan":"false"}')
                                                })


if __name__ == '__main__':
    unittest.main()