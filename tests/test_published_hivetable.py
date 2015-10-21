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

class TestPublishedHivetable(QdsCliTestCase):

    def test_create(self):
        sys.argv = ['qds.py', 'published_hivetable', 'publish', '--space_id', '1', '--table_name', 'test_table', '--schema_name', 'default']
        print_command()
        Connection._api_call = Mock(return_value={"published_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("POST", "published_hivetables", {'space_id': '1', 'table_name': 'test_table', 'schema_name': 'default'})

    def test_list(self):
        sys.argv = ['qds.py', 'published_hivetable', 'list']
        print_command()
        Connection._api_call = Mock(return_value={"published_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "published_hivetables", params=None)

    def test_view(self):
        sys.argv = ['qds.py', 'published_hivetable', 'view', '1']
        print_command()
        Connection._api_call = Mock(return_value={"published_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("GET", "published_hivetables/1", params=None)

    def test_update(self):
        sys.argv = ['qds.py', 'published_hivetable', 'update', '1']
        print_command()
        Connection._api_call = Mock(return_value={"published_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "published_hivetables/1", {})

    def test_delete(self):
        sys.argv = ['qds.py', 'published_hivetable', 'unpublish', '1']
        print_command()
        Connection._api_call = Mock(return_value={"published_hivetable": []})
        qds.main()
        Connection._api_call.assert_called_with("DELETE", "published_hivetables/1", None)

if __name__ == '__main__':
    unittest.main()
