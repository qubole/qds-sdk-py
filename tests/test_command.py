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
import qds_sdk
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase


class TestCommandCheck(QdsCliTestCase):

    def test_hivecmd(self):
        sys.argv = ['qds.py', 'hivecmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params=None)

    def test_hadoopcmd(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params=None)

    def test_prestocmd(self):
        sys.argv = ['qds.py', 'prestocmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params=None)

    def test_pigcmd(self):
        sys.argv = ['qds.py', 'pigcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params=None)

    def test_shellcmd(self):
        sys.argv = ['qds.py', 'shellcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params=None)

    def test_dbexportcmd(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params=None)

    def test_dbimportcmd(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params=None)

    def test_dbtapquerycmd(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params=None)


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

    def test_dbimportcmd(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})

    def test_dbtapquerycmd(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})


class TestHiveCommand(QdsCliTestCase):

    def test_submit_query(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'sample_size': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None})

    def test_submit_script_location(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--script_location', 's3://bucket/path-to-script']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'sample_size': None,
                 'query': None,
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script'})

    def test_submit_none(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_both(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--script_location', 's3://bucket/path-to-script']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_macros(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--script_location', 's3://bucket/path-to-script',
                    '--macros', '[{"key1":"11","key2":"22"}, {"key3":"key1+key2"}]']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': [{"key1":"11","key2":"22"}, {"key3":"key1+key2"}],
                 'label': None,
                 'sample_size': None,
                 'query': None,
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script'})

    def test_submit_cluster_label(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--cluster-label', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': 'test_label',
                 'sample_size': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None})

    def test_submit_notify(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--notify']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'sample_size': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': True,
                 'script_location': None})

    def test_submit_sample_size(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--sample_size', '1024']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'sample_size': '1024',
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None})


class TestPrestoCommand(QdsCliTestCase):

    def test_submit_query(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'query': 'show tables',
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': None})

    def test_submit_script_location(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--script_location', 's3://bucket/path-to-script']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'query': None,
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script'})

    def test_submit_none(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_both(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables',
                    '--script_location', 's3://bucket/path-to-script']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_macros(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--script_location', 's3://bucket/path-to-script',
                    '--macros', '[{"key1":"11","key2":"22"}, {"key3":"key1+key2"}]']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': [{"key1":"11","key2":"22"}, {"key3":"key1+key2"}],
                 'label': None,
                 'query': None,
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script'})

    def test_submit_cluster_label(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables',
                    '--cluster-label', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': 'test_label',
                 'query': 'show tables',
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': None})

    def test_submit_notify(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables',
                    '--notify']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'query': 'show tables',
                 'command_type': 'PrestoCommand',
                 'can_notify': True,
                 'script_location': None})


class TestHadoopCommand(QdsCliTestCase):

    def test_submit_jar(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', 'jar', 's3://bucket/path-to-jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'jar',
                 'sub_command_args': "'s3://bucket/path-to-jar'",
                 'label': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False})

    def test_submit_jar_invalid(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', 'jar']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_s3distcp(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', 's3distcp', '--src', 'source', '--dest', 'destincation']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 's3distcp',
                 'sub_command_args': "'--src' 'source' '--dest' 'destincation'",
                 'label': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False})

    def test_submit_s3distcp_invalid(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', 's3distcp']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_streaming(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', 'streaming',
                    '-files', 's3n://location-of-mapper.py,s3n://location-of-reducer.py',
                    '-input', 'myInputDirs',
                    '-output', 'myOutputDir',
                    '-mapper', 'mapper.py',
                    '-reducer', 'reducer.py']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'streaming',
                 'sub_command_args': "'-files' 's3n://location-of-mapper.py,s3n://location-of-reducer.py' '-input' 'myInputDirs' '-output' 'myOutputDir' '-mapper' 'mapper.py' '-reducer' 'reducer.py'",
                 'label': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False})

    def test_submit_streaming_invalid(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', 'streaming']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_jar_cluster_label(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', '--cluster-label', 'test_label', 'jar', 's3://bucket/path-to-jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'jar',
                 'sub_command_args': "'s3://bucket/path-to-jar'",
                 'label': 'test_label',
                 'command_type': 'HadoopCommand',
                 'can_notify': False})

    def test_submit_jar_notify(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', '--notify', 'jar', 's3://bucket/path-to-jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'jar',
                 'sub_command_args': "'s3://bucket/path-to-jar'",
                 'label': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': True})


class TestShellCommand(QdsCliTestCase):

    def test_stub(self):
        pass


class TestPigCommand(QdsCliTestCase):

    def test_stub(self):
        pass


class TestDbExportCommand(QdsCliTestCase):

    def test_stub(self):
        pass


class TestDbImportCommand(QdsCliTestCase):

    def test_stub(self):
        pass


class TestDbTapQueryCommand(QdsCliTestCase):

    def test_stub(self):
        pass


if __name__ == '__main__':
    unittest.main()
