import sys
import os

if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import *

sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from mock import Mock, ANY
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase


class TestConnection(QdsCliTestCase):

	#Test with correct values set
    def test_connection_object(self):
        sys.argv = ['qds.py', '--max_retries', '3', '--base_retry_delay', '2', 'cluster', 'list']
        print_command()
        Connection.__init__ = Mock(return_value=None)
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection.__init__.assert_called_with(ANY, ANY, ANY, ANY, 3, 2)

    #Test with incorrect values
    def test_connection_override(self):
        sys.argv = ['qds.py', '--max_retries', '15', '--base_retry_delay', '15', 'cluster', 'list']
        print_command()
        Connection.__init__ = Mock(return_value=None)
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection.__init__.assert_called_with(ANY, ANY, ANY, ANY, 7, 10)

    #Test with no values given should set default
    def test_connection_default(self):
        sys.argv = ['qds.py', 'cluster', 'list']
        print_command()
        Connection.__init__ = Mock(return_value=None)
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection.__init__.assert_called_with(ANY, ANY, ANY, ANY, 7, 10)

if __name__ == '__main__':
    unittest.main()
