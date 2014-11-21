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

    def test_update_group(self):
        sys.argv = ['qds.py', 'group', 'update', '123', '--name', 'sdk-test']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/123", {'name':'sdk-test'}))

    def test_delete_group(self):
        sys.argv = ['qds.py', 'group', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("DELETE", "groups/123", None))

    def test_duplicate(self):
        sys.argv = ['qds.py', 'group', 'duplicate', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("POST", "groups/123/duplicate", {}))

    def test_add_user(self):
        sys.argv = ['qds.py', 'group', 'add_user', '123', '--user_id', '456']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/123/qbol_users/456/add", None))

    def test_remove_user(self):
        sys.argv = ['qds.py', 'group', 'remove_user', '123', '--user_id',
        '456']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/123/qbol_users/456/remove", None))

    def test_list_roles(self):
        sys.argv = ['qds.py', 'group', 'list_roles', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "groups/123/roles", params=None))

    def test_list_users(self):
        sys.argv = ['qds.py', 'group', 'list_users', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("GET", "groups/123/qbol_users", params=None))

    def test_add_role(self):
        sys.argv = ['qds.py', 'group', 'add_role', '123', '--role_id', '456']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/123", {'roles':'456'}))

    def test_remove_role(self):
        sys.argv = ['qds.py', 'group', 'remove_role', '123', '--role_id', '456']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_has_calls(call("PUT", "groups/123", {'removed_roles':'456'}))


if __name__ == '__main__':
    unittest.main()

