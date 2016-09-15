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


class TestInviteUser(QdsCliTestCase):
    def test_invite_without_group(self):
        sys.argv = ['qds.py', 'user', 'invite',
                    '--email', 'mock@qubole1.com',
                    '--account-id', '34']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "users/invite_new", {
            'invitee_email': 'mock@qubole1.com',
            'account': '34',
            'groups': None})

    def test_invite_with_group(self):
        sys.argv = ['qds.py', 'user', 'invite',
                    '--email', 'mock@qubole1.com',
                    '--account-id', '34',
                    '--groups', "system-admin"]
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "users/invite_new", {
            'invitee_email': 'mock@qubole1.com',
            'account': '34',
            'groups': 'system-admin'})

    def test_invite_with_multiple_groups(self):
        sys.argv = ['qds.py', 'user', 'invite',
                    '--email', 'mock@qubole1.com',
                    '--account-id', '34',
                    '--groups', "system-admin,system-user"]
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "users/invite_new", {
            'invitee_email': 'mock@qubole1.com',
            'account': '34',
            'groups': 'system-admin,system-user'})

    def test_invite_no_email(self):
        sys.argv = ['qds.py', 'user', 'invite',
                    '--account-id', '34',
                    '--groups', "system-admin"]
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_invite_no_accountid(self):
        sys.argv = ['qds.py', 'user', 'invite',
                    'invitee_email', 'mock@qubole1.com',
                    '--groups', "system-admin"]
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()


class TestEnableUser(QdsCliTestCase):

    def test_enable_user_valid(self):
        sys.argv = ['qds.py', 'user', 'enable',
                    '--qbol-user-id', '4']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "accounts/enable_qbol_user", {
            'qbol_user_id': '4'})

    def test_enable_user_invalid(self):
        sys.argv = ['qds.py', 'user', 'enable']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()


class TestDisableUser(QdsCliTestCase):
    def test_disable_user_valid(self):
        sys.argv = ['qds.py', 'user', 'disable',
                    '--qbol-user-id', '4']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "accounts/disable_qbol_user", {
            'qbol_user_id': '4'})

    def test_disable_user_invalid(self):
        sys.argv = ['qds.py', 'user', 'disable']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()


if __name__ == '__main__':
    unittest.main()
