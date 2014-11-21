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
from qds_sdk.role import *
from test_base import print_command
from test_base import QdsCliTestCase

class TestRoleCheck(QdsCliTestCase):

    def test_list(self):
        sys.argv = ['qds.py', 'role', 'list']
        print_command()
        Connection._api_call = Mock(return_value={'roles':[]})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "roles", params=None))

    def test_view(self):
        sys.argv = ['qds.py', 'role', 'view', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "roles/123", params=None))

    def test_update(self):
        sys.argv = ['qds.py', 'role', 'update', '123', '--name', 'test']
        print_command()
        Connection._api_call = Mock(return_value={'roles':[]})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "roles/123", {'name':'test'}))

    def test_delete(self):
        sys.argv = ['qds.py', 'role', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("DELETE", "roles/123", None))

    def test_duplicate(self):
        sys.argv = ['qds.py', 'role', 'duplicate', '123']
        print_command()
        Connection._api_call = Mock(return_value={'roles':[]})
        qds.main()
        Connection._api_call.assert_has_calls(call("POST", "roles/123/duplicate", {}))

    def test_assign_role(self):
        sys.argv = ['qds.py', 'role', 'assign_role', '123', '--group_id',
        '456']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/456/roles/123/assign", None))

    def test_unassign_role(self):
        sys.argv = ['qds.py', 'role', 'unassign_role', '123', "--group_id", "456"]
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/456/roles/123/unassign", None))

    def test_list_groups(self):
        sys.argv = ['qds.py', 'role', 'list_groups', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "roles/123/groups", params=None))

if __name__ == '__main__':
    unittest.main()

