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
from qds_sdk.group import *
from test_base import print_command
from test_base import QdsCliTestCase

class TestGroupCheck(QdsCliTestCase):

    def test_list(self):
        sys.argv = ['qds.py', 'group', 'list']
        print_command()
        Connection._api_call = Mock(return_value={'groups':[]})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "groups", params=None))

    def test_view(self):
        sys.argv = ['qds.py', 'group', 'view', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "groups/123", params=None))

    def test_view_neg(self):
        sys.argv = ['qds.py', 'group', 'view']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_update_group(self):
        sys.argv = ['qds.py', 'group', 'update', '123', '--name', 'sdk-test', '--members', '7,8,9', '--roles', '10,11', '--remove-members', '5,6', '--remove-roles', '12,13']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/123", {'members': '7,8,9', 'removed_roles': '12,13', 'removed_members': '5,6', 'name': 'sdk-test', 'roles': '10,11'}))

    def test_update_neg(self):
        sys.argv = ['qds.py', 'group', 'update']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_delete_group(self):
        sys.argv = ['qds.py', 'group', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("DELETE", "groups/123", None))

    def test_delete_neg(self):
        sys.argv = ['qds.py', 'group', 'delete']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_duplicate(self):
        sys.argv = ['qds.py', 'group', 'duplicate', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("POST", "groups/123/duplicate", {}))

    def test_duplicate_with_name(self):
        sys.argv = ['qds.py', 'group', 'duplicate', '123', '--name', 'duplicate']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("POST", "groups/123/duplicate", {'name':'duplicate'}))

    def test_duplicate_neg(self):
        sys.argv = ['qds.py', 'group', 'duplicate']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_add_users(self):
        sys.argv = ['qds.py', 'group', 'add-users', '123', '456,789']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call('PUT', 'groups/123', {'members': '456,789'}))

    def test_add_users_neg(self):
        sys.argv = ['qds.py', 'group', 'add-users', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_remove_users(self):
        sys.argv = ['qds.py', 'group', 'remove-users', '123', '456,789']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call('PUT', 'groups/123', {'removed_members': '456,789'}))

    def test_remove_users_neg(self):
        sys.argv = ['qds.py', 'group', 'remove-users', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_list_roles(self):
        sys.argv = ['qds.py', 'group', 'list-roles', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "groups/123/roles", params=None))

    def test_list_roles_neg(self):
        sys.argv = ['qds.py', 'group', 'list-roles']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_list_users(self):
        sys.argv = ['qds.py', 'group', 'list-users', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "groups/123/qbol_users", params=None))

    def test_list_users_neg(self):
        sys.argv = ['qds.py', 'group', 'list-users']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_add_roles(self):
        sys.argv = ['qds.py', 'group', 'add-roles', '123', '456,789']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/123", {'roles':'456,789'}))

    def test_add_roles_neg(self):
        sys.argv = ['qds.py', 'group', 'add-roles', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_remove_roles(self):
        sys.argv = ['qds.py', 'group', 'remove-roles', '123', '456,789']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/123", {'removed_roles':'456,789'}))

    def test_remove_roles_neg(self):
        sys.argv = ['qds.py', 'group', 'remove-roles', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

if __name__ == '__main__':
    unittest.main()

