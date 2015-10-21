from __future__ import print_function
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

class TestSubscribedHivetable(QdsCliTestCase):

    def test_create(self):
        sys.argv = ['qds.py', 'subscribed_hivetable', 'subscribe', '--published_hivetable_id', '1', '--schema_name', 'default']
        print_command()
        Connection._api_call = Mock(return_value={"subscribed_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("POST", "subscribed_hivetables", {'published_hivetable_id': '1', 'schema_name': 'default'})

    def test_list(self):
        sys.argv = ['qds.py', 'subscribed_hivetable', 'list']
        print_command()
        Connection._api_call = Mock(return_value={"subscribed_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "subscribed_hivetables", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'subscribed_hivetable', 'view', '1']
        print_command()
        Connection._api_call = Mock(return_value={"subscribed_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "subscribed_hivetables/1", params=None)

    def test_update(self):
        sys.argv = ['qds.py', 'subscribed_hivetable', 'update', '1']
        print_command()
        Connection._api_call = Mock(return_value={"subscribed_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "subscribed_hivetables/1", {})

    def test_delete(self):
        sys.argv = ['qds.py', 'subscribed_hivetable', 'unsubscribe', '1']
        print_command()
        Connection._api_call = Mock(return_value={"subscribed_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("DELETE", "subscribed_hivetables/1", params=None)

if __name__ == '__main__':
    unittest.main()