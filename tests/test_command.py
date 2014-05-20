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


class TestCommandCheck(QdsCliTestCase):

    def test_hivecmd(self):
        sys.argv = ['qds.py', 'hivecmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", None)

    def test_hadoopcmd(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", None)

    def test_prestocmd(self):
        sys.argv = ['qds.py', 'prestocmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", None)

    def test_pigcmd(self):
        sys.argv = ['qds.py', 'pigcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", None)

    def test_shellcmd(self):
        sys.argv = ['qds.py', 'shellcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", None)

    def test_dbexportcmd(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", None)


class TestCommandCancel(QdsCliTestCase):

    def test_hivecmd(self):
        sys.argv = ['qds.py', 'hivecmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})

    def test_hadoopcmd(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})

    def test_prestocmd(self):
        sys.argv = ['qds.py', 'prestocmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})

    def test_pigcmd(self):
        sys.argv = ['qds.py', 'pigcmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})

    def test_shellcmd(self):
        sys.argv = ['qds.py', 'shellcmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})

    def test_dbexportcmd(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})


if __name__ == '__main__':
    unittest.main()
