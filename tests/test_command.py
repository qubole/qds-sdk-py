from __future__ import print_function
import sys
import os
import pytest
if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import *
from tempfile import NamedTemporaryFile
sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
import qds_sdk
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase


class TestCommandList(QdsCliTestCase):

    def test_list_minimal(self):
        sys.argv = ['qds.py', 'shellcmd', 'list']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = None
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands", params=params)

    def test_list_page(self):
        sys.argv = ['qds.py', 'shellcmd', 'list', '--page', '2']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'page': 2}
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands", params=params)

    def test_list_per_page(self):
        sys.argv = ['qds.py', 'shellcmd', 'list', '--per-page', '5']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'per_page': 5}
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands", params=params)

    def test_list_all_users(self):
        sys.argv = ['qds.py', 'shellcmd', 'list', '--all-users', '1']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'all_users': 1}
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands", params=params)

    def test_list_include_query_properties(self):
        sys.argv = ['qds.py', 'shellcmd', 'list', '--include-query-properties']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'include_query_properties': True}
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands", params=params)

    def test_list_start_date(self):
        sys.argv = ['qds.py', 'shellcmd', 'list', '--start-date', '2019-01-22T15:11:00Z']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'start_date': '2019-01-22T15:11:00Z'}
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands", params=params)

    def test_list_end_date(self):
        sys.argv = ['qds.py', 'shellcmd', 'list', '--end-date', '2019-01-22T15:11:00Z']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'end_date': '2019-01-22T15:11:00Z'}
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands", params=params)

class TestCommandCheck(QdsCliTestCase):

    @patch("qds.print",create=True)
    def test_hivecmd(self, print_):
        sys.argv = ['qds.py', 'hivecmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={u'command_source': u'API'})
        qds.main()
        print_.assert_called_with('{"command_source": "API"}')
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_sparkcmd(self):
        sys.argv = ['qds.py', 'sparkcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_hadoopcmd(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_prestocmd(self):
        sys.argv = ['qds.py', 'prestocmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_pigcmd(self):
        sys.argv = ['qds.py', 'pigcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_shellcmd(self):
        sys.argv = ['qds.py', 'shellcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_dbexportcmd(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_dbimportcmd(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_dbtapquerycmd(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_jupyternotebookcmd(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'check', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'false'})

    def test_includequeryproperty(self):
        sys.argv = ['qds.py', 'hivecmd', 'check', '123', 'true']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "commands/123", params={'include_query_properties': 'true'})



class TestCommandCancel(QdsCliTestCase):

    def test_hivecmd(self):
        sys.argv = ['qds.py', 'hivecmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})

    def test_sparkcmd(self):
        sys.argv = ['qds.py', 'sparkcmd', 'cancel', '123']
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

    def test_jupyternotebookcmd(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'cancel', '123']
        print_command()
        Connection._api_call = Mock(return_value={'kill_succeeded': True})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "commands/123",
                {'status': 'kill'})


class TestCommandGetJobs(QdsCliTestCase):

    def test_running(self):
        sys.argv = ['qds.py', 'hivecmd', 'getjobs', '123']
        print_command()
        Connection._api_call = Mock(return_value={'id':123, 'status': 'running'})
        Connection._api_call_raw = Mock()
        qds.main()
        Connection._api_call.assert_called_with('GET', 'commands/123', params=None),
        assert not Connection._api_call_raw.called

    def test_done(self):
        sys.argv = ['qds.py', 'hivecmd', 'getjobs', '123']
        print_command()
        Connection._api_call = Mock(return_value={'id':123, 'status': "done"})
        jobs_response = Mock(text='[{"url":"https://blah","job_stats":{},"job_id":"job_blah"}]')
        Connection._api_call_raw = Mock(return_value=jobs_response)
        qds.main()
        Connection._api_call.assert_called_with('GET', 'commands/123', params=None),
        Connection._api_call_raw.assert_called_with('GET', 'commands/123/jobs', params=None),


class TestHiveCommand(QdsCliTestCase):

    def test_submit_query(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables', '--retry', 2]
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'hive_version': None,
                 'label': None,
                 'tags': None,
                 'sample_size': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None,
                 'retry': 2,
                 'pool': None})

    def test_submit_query_with_hive_version(self):
            sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables', '--hive-version', '0.13']
            print_command()
            Connection._api_call = Mock(return_value={'id': 1234})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'commands',
                    {'macros': None,
                     'hive_version': '0.13',
                     'label': None,
                     'tags': None,
                     'sample_size': None,
                     'name': None,
                     'query': 'show tables',
                     'command_type': 'HiveCommand',
                     'can_notify': False,
                     'script_location': None,
                     'retry': 0,
                     'pool': None})

    def test_submit_script_location(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--script_location', 's3://bucket/path-to-script']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'hive_version': None,
                 'label': None,
                 'tags': None,
                 'sample_size': None,
                 'name': None,
                 'query': None,
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script',
                 'retry': 0,
                 'pool': None})

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
                 'hive_version': None,
                 'label': None,
                 'tags': None,
                 'sample_size': None,
                 'name': None,
                 'query': None,
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script',
                 'retry': 0,
                 'pool': None})

    def test_submit_tags(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--script_location', 's3://bucket/path-to-script',
                    '--tags', 'abc,def']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'hive_version': None,
                 'label': None,
                 'tags': ["abc", "def"],
                 'sample_size': None,
                 'name': None,
                 'query': None,
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script',
                 'retry': 0,
                 'pool': None})

    def test_submit_cluster_label(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--cluster-label', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'hive_version': None,
                 'label': 'test_label',
                 'tags': None,
                 'sample_size': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None,
                 'retry': 0,
                 'pool': None})

    def test_submit_name(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--name', 'test_name']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'hive_version': None,
                 'label': None,
                 'tags': None,
                 'sample_size': None,
                 'name': 'test_name',
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None,
                 'retry': 0,
                 'pool': None})

    def test_submit_notify(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--notify']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'hive_version': None,
                 'label': None,
                 'tags': None,
                 'sample_size': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': True,
                 'script_location': None,
                 'retry': 0,
                 'pool': None})

    def test_submit_sample_size(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--sample_size', '1024']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'hive_version': None,
                 'label': None,
                 'tags': None,
                 'sample_size': '1024',
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None,
                 'retry': 0,
                 'pool': None})

    def test_retry_out_of_range(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--retry', 4]
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_pool(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables', '--pool', 'batch']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'hive_version': None,
                 'label': None,
                 'tags': None,
                 'sample_size': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None,
                 'retry': 0,
                 'pool': 'batch'})

class TestSparkCommand(QdsCliTestCase):

    def test_submit_query(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--cmdline', '/usr/lib/spark/bin/spark-submit --class Test Test.jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': None,
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': None,
                 'app_id': None,
                 'cmdline':'/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_notebook(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--note-id', '111','--name', 'notebook-cmd']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': None,
                 'tags': None,
                 'name': 'notebook-cmd',
                 'sql': None,
                 'program': None,
                 'app_id': None,
                 'cmdline':None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : '111',
                 'retry': 0,
                 'pool': None})

    def test_submit_notebook_with_cmdline(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--note-id', '111', '--cmdline', 'ls']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()
    def test_submit_notebook_with_program(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--note-id', '111', '--program', 'print "hello"']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_script_location_aws_python(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--script_location', 's3://bucket/path-to-script.py']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': "python",
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': None,
                 'app_id': None,
                 'cmdline':None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script.py',
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_script_location_aws_scala(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--script_location', 's3://bucket/path-to-script.scala']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': "scala",
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': None,
                 'app_id': None,
                 'cmdline':None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script.scala',
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_script_location_aws_R(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--script_location', 's3://bucket/path-to-script.R']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': "R",
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': None,
                 'app_id': None,
                 'cmdline':None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script.R',
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_script_location_aws_sql(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--script_location', 's3://bucket/path-to-script.sql']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': "sql",
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': None,
                 'app_id': None,
                 'cmdline':None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script.sql',
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_script_location_aws_java(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--script_location', 's3://bucket/path-to-script.java']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_script_location_local_py(self):
        with NamedTemporaryFile(suffix=".py") as tmp:
            tmp.write('print "Hello World!"'.encode("utf8"))
            tmp.seek(0)
            sys.argv = ['qds.py', 'sparkcmd' , 'submit', '--script_location' , tmp.name]
            print_command()
            Connection._api_call = Mock(return_value={'id': 1234})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'commands',
                    {'macros': None,
                     'label': None,
                     'language': "python",
                     'tags': None,
                     'name': None,
                     'sql': None,
                     'program':'print "Hello World!"',
                     'app_id': None,
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None,
                     'note_id' : None,
                     'retry': 0,
                     'pool': None})

    def test_submit_script_location_local_scala(self):
        with NamedTemporaryFile(suffix=".scala") as tmp:
            tmp.write('println("hello, world!")'.encode("utf8"))
            tmp.seek(0)
            sys.argv = ['qds.py', 'sparkcmd' , 'submit', '--script_location' , tmp.name]
            print_command()
            Connection._api_call = Mock(return_value={'id': 1234})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'commands',
                    {'macros': None,
                     'label': None,
                     'language': "scala",
                     'tags': None,
                     'name': None,
                     'sql': None,
                     'program': "println(\"hello, world!\")",
                     'app_id': None,
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None,
                     'note_id' : None,
                     'retry': 0,
                     'pool': None})

    def test_submit_script_location_local_java(self):
        with NamedTemporaryFile(suffix=".java") as tmp:
            tmp.write('println("hello, world!")'.encode("utf8"))
            tmp.seek(0)
            sys.argv = ['qds.py', 'sparkcmd' , 'submit', '--script_location' , tmp.name]
            print_command()
            with self.assertRaises(qds_sdk.exception.ParseError):
                qds.main()

    def test_submit_script_location_local_R(self):
        with NamedTemporaryFile(suffix=".R") as tmp:
            tmp.write('cat("hello, world!")'.encode("utf8"))
            tmp.seek(0)
            sys.argv = ['qds.py', 'sparkcmd' , 'submit', '--script_location' , tmp.name]
            print_command()
            Connection._api_call = Mock(return_value={'id': 1234})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'commands',
                    {'macros': None,
                     'label': None,
                     'language': "R",
                     'tags': None,
                     'name': None,
                     'sql': None,
                     'program': "cat(\"hello, world!\")",
                     'app_id': None,
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None,
                     'note_id' : None,
                     'retry': 0,
                     'pool': None})

    def test_submit_script_location_local_sql(self):
        with NamedTemporaryFile(suffix=".sql") as tmp:
            tmp.write('show tables'.encode("utf8"))
            tmp.seek(0)
            sys.argv = ['qds.py', 'sparkcmd', 'submit', '--script_location', tmp.name ]
            print_command()
            Connection._api_call = Mock(return_value={'id': 1234})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'commands',
                    {'macros': None,
                     'label': None,
                     'language': None,
                     'tags': None,
                     'name': None,
                     'sql': "show tables",
                     'program': None,
                     'app_id': None,
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None,
                     'note_id' : None,
                     'retry': 0,
                     'pool': None})

    def test_submit_sql(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--sql', 'show dummy']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                    {'macros': None,
                     'label': None,
                     'language': None,
                     'tags': None,
                     'name': None,
                     'sql': 'show dummy',
                     'program': None,
                     'app_id': None,
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None,
                     'note_id' : None,
                     'retry': 0,
                     'pool': None})

    def test_submit_sql_with_language(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--language','python', '--sql', 'show dummy']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_none(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_both(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--cmdline', '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                    '--script_location', 'home/path-to-script']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_all_three(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--cmdline', '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                    '--script_location', '/home/path-to-script', 'program', 'println("hello, world!")']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_language(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--program', 'println("hello, world!")',
                    '--language', 'java']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_program_no_language(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--program', 'println("hello, world!")']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_macros(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--program',"println(\"hello, world!\")" ,'--language', 'scala',
                    '--macros', '[{"key1":"11","key2":"22"}, {"key3":"key1+key2"}]']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': [{"key1":"11","key2":"22"}, {"key3":"key1+key2"}],
                 'label': None,
                 'language': "scala",
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'arguments': None,
                 'user_program_arguments': None,
                 'program': "println(\"hello, world!\")",
                 'app_id': None,
                 'command_type': 'SparkCommand',
                 'cmdline': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_tags(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--language','scala','--program',"println(\"hello, world!\")",
                    '--tags', 'abc,def']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': 'scala',
                 'tags': ["abc", "def"],
                 'name': None,
                 'sql': None,
                 'program':"println(\"hello, world!\")" ,
                 'app_id': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'cmdline': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_cluster_label(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--cmdline', '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                    '--cluster-label', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': 'test_label',
                 'language' : None,
                 'cmdline': '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program' : None,
                 'app_id': None,
                 'arguments': None,
                 'user_program_arguments': None,
                 'command_type': 'SparkCommand',
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_name(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--cmdline', '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                    '--name', 'test_name']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language' : None,
                 'cmdline' : '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                 'tags': None,
                 'name': 'test_name',
                 'sql': None,
                 'arguments': None,
                 'user_program_arguments': None,
                 'program': None,
                 'app_id': None,
                 'command_type': 'SparkCommand',
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_notify(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--cmdline', '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                    '--notify']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language' : None,
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': None,
                 'app_id': None,
                 'cmdline': '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': True,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_python_program(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--language','python','--program', 'print "hello, world!"']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language' : 'python',
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': "print \"hello, world!\"",
                 'app_id': None,
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_user_program_arguments(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--language','scala','--program',
                    "object HelloWorld {\n\n    def main(args: Array[String]) {\n        \n        println(\"Hello, \" + args(0))\n    \n    }\n}\n",
                    '--arguments', '--class HelloWorld',
                    '--user_program_arguments', 'world']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language' : 'scala',
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': "object HelloWorld {\n\n    def main(args: Array[String]) {\n        \n        println(\"Hello, \" + args(0))\n    \n    }\n}\n" ,
                 'app_id': None,
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': '--class HelloWorld',
                 'user_program_arguments': 'world',
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_scala_program(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--language','scala','--program', 'println("hello, world!")']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language' : 'scala',
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': "println(\"hello, world!\")",
                 'app_id': None,
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_R_program(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--language','R','--program', 'cat("hello, world!")']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language' : 'R',
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': "cat(\"hello, world!\")",
                 'app_id': None,
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_program_to_app(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--language', 'scala',
                    '--program', 'sc.version', '--app-id', '1']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': 'scala',
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': "sc.version",
                 'app_id': 1,
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_sql_to_app(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--sql', 'show tables',
                    '--app-id', '1']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': None,
                 'tags': None,
                 'name': None,
                 'sql': 'show tables',
                 'program': None,
                 'app_id': 1,
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': None})

    def test_submit_script_location_local_py_to_app(self):
        with NamedTemporaryFile(suffix=".py") as tmp:
            tmp.write('print "Hello World!"'.encode("utf8"))
            tmp.seek(0)
            sys.argv = ['qds.py', 'sparkcmd', 'submit',
                        '--script_location', tmp.name, '--app-id', '1']
            print_command()
            Connection._api_call = Mock(return_value={'id': 1234})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'commands',
                    {'macros': None,
                     'label': None,
                     'language': "python",
                     'tags': None,
                     'name': None,
                     'sql': None,
                     'program':'print "Hello World!"',
                     'app_id': 1,
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None,
                     'note_id' : None,
                     'retry': 0,
                     'pool': None})

    def test_submit_cmdline_to_app(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--cmdline',
                    '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                    '--app-id', '1']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_pool(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--cmdline',
                    '/usr/lib/spark/bin/spark-submit --class Test Test.jar', '--pool', 'batch']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'language': None,
                 'tags': None,
                 'name': None,
                 'sql': None,
                 'program': None,
                 'app_id': None,
                 'cmdline':'/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None,
                 'note_id' : None,
                 'retry': 0,
                 'pool': 'batch'})

class TestPrestoCommand(QdsCliTestCase):

    def test_submit_query(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables', '--retry', 1]
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'tags': None,
                 'label': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': None,
                 'retry': 1})

    def test_submit_script_location(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--script_location', 's3://bucket/path-to-script']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'tags': None,
                 'name': None,
                 'query': None,
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script',
                 'retry': 0})

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
                 'tags': None,
                 'label': None,
                 'name': None,
                 'query': None,
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script',
                 'retry': 0})

    def test_submit_tags(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--script_location', 's3://bucket/path-to-script',
                    '--tags', 't1,t2']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'tags': ["t1", "t2"],
                 'label': None,
                 'name': None,
                 'query': None,
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script',
                 'retry': 0})

    def test_submit_cluster_label(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables',
                    '--cluster-label', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': 'test_label',
                 'tags': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': None,
                 'retry': 0})

    def test_submit_name(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables',
                    '--name', 'test_name']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'tags': None,
                 'label': None,
                 'name': 'test_name',
                 'query': 'show tables',
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': None,
                 'retry': 0})

    def test_submit_notify(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables',
                    '--notify']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'tags': None,
                 'label': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'PrestoCommand',
                 'can_notify': True,
                 'script_location': None,
                 'retry': 0})

    def test_retry_out_of_range(self):
        sys.argv = ['qds.py', 'prestocmd', 'submit', '--query', 'show tables',
                    '--retry', 5]
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

class TestHadoopCommand(QdsCliTestCase):

    def test_submit_jar(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', 'jar', 's3://bucket/path-to-jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'jar',
                 'sub_command_args': "'s3://bucket/path-to-jar'",
                 'name': None,
                 'label': None,
                 'tags': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False,
                 'pool': None})

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
                 'name': None,
                 'label': None,
                 'tags': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False,
                 'pool': None})

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
                 'name': None,
                 'label': None,
                 'tags': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False,
                 'pool': None})

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
                 'name': None,
                 'label': 'test_label',
                 'tags': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False,
                 'pool': None})

    def test_submit_jar_name(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', '--name', 'test_name', 'jar', 's3://bucket/path-to-jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'jar',
                 'sub_command_args': "'s3://bucket/path-to-jar'",
                 'name': 'test_name',
                 'label': None,
                 'tags': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False,
                 'pool': None})

    def test_submit_jar_notify(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', '--notify', 'jar', 's3://bucket/path-to-jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'jar',
                 'sub_command_args': "'s3://bucket/path-to-jar'",
                 'name': None,
                 'label': None,
                 'tags': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': True,
                 'pool': None})

    def test_submit_tags(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', '--name', 'test_name',  '--tags', 'abc,def', 'jar', 's3://bucket/path-to-jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'jar',
                 'sub_command_args': "'s3://bucket/path-to-jar'",
                 'name': 'test_name',
                 'tags': ['abc', 'def'],
                 'label': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False,
                 'pool': None})

    def test_submit_pool(self):
        sys.argv = ['qds.py', 'hadoopcmd', 'submit', '--pool', 'batch', 'jar', 's3://bucket/path-to-jar']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'sub_command': 'jar',
                 'sub_command_args': "'s3://bucket/path-to-jar'",
                 'name': None,
                 'label': None,
                 'tags': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False,
                 'pool': 'batch'})


class TestShellCommand(QdsCliTestCase):

    def test_stub(self):
        pass

    def test_submit_inline(self):
        sys.argv = ['qds.py', 'shellcmd', 'submit', '--script', 'ls /tmp']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'inline': 'ls /tmp',
                                                 'files': None,
                                                 'name': None,
                                                 'tags': None,
                                                 'script_location': None,
                                                 'label': None,
                                                 'archives': None,
                                                 'command_type': 'ShellCommand',
                                                 'can_notify': False,
                                                 'pool': None})

    def test_submit_pool(self):
        sys.argv = ['qds.py', 'shellcmd', 'submit', '--script', 'ls /tmp', '--pool', 'batch']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'inline': 'ls /tmp',
                                                 'files': None,
                                                 'name': None,
                                                 'tags': None,
                                                 'script_location': None,
                                                 'label': None,
                                                 'archives': None,
                                                 'command_type': 'ShellCommand',
                                                 'can_notify': False,
                                                 'pool': 'batch'})

class TestPigCommand(QdsCliTestCase):

    def test_stub(self):
        pass

    def test_submit_latin_statements(self):
        sys.argv = ['qds.py', 'pigcmd', 'submit',
                    '--script', 'A = LOAD "s3://xxx/yyy.log"; dump A;']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'retry': 0,
                                                 'name': None,
                                                 'tags': None,
                                                 'label': None,
                                                 'script_location': None,
                                                 'command_type': 'PigCommand',
                                                 'latin_statements': 'A = LOAD "s3://xxx/yyy.log"; dump A;',
                                                 'can_notify': False,
                                                 'pool': None})

    def test_submit_pool(self):
        sys.argv = ['qds.py', 'pigcmd', 'submit',
                    '--script', 'A = LOAD "s3://xxx/yyy.log"; dump A;',
                    '--pool', 'batch']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'retry': 0,
                                                 'name': None,
                                                 'tags': None,
                                                 'label': None,
                                                 'script_location': None,
                                                 'command_type': 'PigCommand',
                                                 'latin_statements': 'A = LOAD "s3://xxx/yyy.log"; dump A;',
                                                 'can_notify': False,
                                                 'pool': 'batch'})


class TestDbExportCommand(QdsCliTestCase):

    def test_submit_command(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--retry', 3]
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': None,
                 'use_customer_cluster': False,
                 'export_dir': None,
                 'name': None,
                 'db_update_keys': None,
                 'partition_spec': None,
                 'fields_terminated_by': None,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'mode': '1',
                 'tags': None,
                 'command_type': 'DbExportCommand',
                 'dbtap_id': '1',
                 'can_notify': False,
                 'db_update_mode': None,
                 'retry': 3,
                 'schema':None,
                 'additional_options':None
                 })

    def test_submit_fail_with_no_parameters(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_with_notify(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--notify']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': None,
                 'use_customer_cluster': False,
                 'export_dir': None,
                 'name': None,
                 'db_update_keys': None,
                 'partition_spec': None,
                 'fields_terminated_by': None,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'mode': '1',
                 'tags': None,
                 'command_type': 'DbExportCommand',
                 'dbtap_id': '1',
                 'can_notify': True,
                 'db_update_mode': None,
                 'retry': 0,
                 'schema': None,
                 'additional_options': None
                 })

    def test_use_customer_cluster_command(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--retry', 3,'--use_customer_cluster',True,'--customer_cluster_label','hadoop1']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': 'hadoop1',
                 'use_customer_cluster': True,
                 'export_dir': None,
                 'name': None,
                 'db_update_keys': None,
                 'partition_spec': None,
                 'fields_terminated_by': None,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'mode': '1',
                 'tags': None,
                 'command_type': 'DbExportCommand',
                 'dbtap_id': '1',
                 'can_notify': False,
                 'db_update_mode': None,
                 'retry': 3,
                 'schema': None,
                 'additional_options': None
                 })

    def test_use_customer_cluster_command_set_false(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--retry', 3,'--use_customer_cluster',False,'--customer_cluster_label','hadoop1']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': 'hadoop1',
                 'use_customer_cluster': False,
                 'export_dir': None,
                 'name': None,
                 'db_update_keys': None,
                 'partition_spec': None,
                 'fields_terminated_by': None,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'mode': '1',
                 'tags': None,
                 'command_type': 'DbExportCommand',
                 'dbtap_id': '1',
                 'can_notify': False,
                 'db_update_mode': None,
                 'retry': 3,
                 'schema': None,
                 'additional_options': None
                 })

    def test_submit_with_name(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--name', 'commandname']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': None,
                 'use_customer_cluster': False,
                 'export_dir': None,
                 'name': 'commandname',
                 'db_update_keys': None,
                 'partition_spec': None,
                 'fields_terminated_by': None,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'mode': '1',
                 'tags': None,
                 'command_type': 'DbExportCommand',
                 'dbtap_id': '1',
                 'can_notify': False,
                 'db_update_mode': None,
                 'retry': 0,
                 'schema': None,
                 'additional_options': None
                 })

    def test_submit_with_update_mode_and_keys(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable',
          '--db_update_mode', 'updateonly', '--db_update_keys', 'key1']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': None,
                 'use_customer_cluster': False,
                 'export_dir': None,
                 'name': None,
                 'db_update_keys': 'key1',
                 'partition_spec': None,
                 'fields_terminated_by': None,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'mode': '1',
                 'tags': None,
                 'command_type': 'DbExportCommand',
                 'dbtap_id': '1',
                 'can_notify': False,
                 'db_update_mode': 'updateonly',
                 'retry': 0,
                 'schema': None,
                 'additional_options': None
                 })

    def test_submit_with_mode_2(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '2', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable',
           '--export_dir', 's3:///export-path/']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': None,
                 'use_customer_cluster': False,
                 'export_dir': 's3:///export-path/',
                 'name': None,
                 'db_update_keys': None,
                 'partition_spec': None,
                 'fields_terminated_by': None,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'mode': '2',
                 'tags': None,
                 'command_type': 'DbExportCommand',
                 'dbtap_id': '1',
                 'can_notify': False,
                 'db_update_mode': None,
                 'retry': 0,
                 'schema': None,
                 'additional_options': None
                 })

    def test_retry_out_of_range(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
                    '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--retry', 5]
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()


class TestDbImportCommand(QdsCliTestCase):

    # Not much point adding more test cases as the semantic check in main code is still remaining.
    # The test cases might give false positivies
    def test_submit_command(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--retry', 2]
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
               { 'customer_cluster_label': None,
                 'use_customer_cluster': False,
                 'db_parallelism': None,
                 'name': None,
                 'dbtap_id': '1',
                 'db_where': None,
                 'db_boundary_query': None,
                 'mode': '1',
                 'tags': None,
                 'command_type': 'DbImportCommand',
                 'db_split_column': None,
                 'can_notify': False,
                 'hive_table': 'myhivetable',
                 'hive_serde': None,
                 'db_table': 'mydbtable',
                 'db_extract_query': None,
                 'retry': 2,
                 'schema': None,
                 'additional_options': None,
                 'part_spec': None
                 })

    def test_use_customer_cluster_command(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
                    '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--retry', 3, '--use_customer_cluster',True,
                     '--customer_cluster_label', 'hadoop2']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': 'hadoop2',
                 'use_customer_cluster': True,
                 'db_parallelism': None,
                 'name': None,
                 'hive_serde': None,
                 'tags': None,
                 'db_where': None,
                 'mode': '1',
                 'db_boundary_query': None,
                 'db_extract_query': None,
                 'db_split_column': None,
                 'retry': 3,
                 'command_type': 'DbImportCommand',
                 'dbtap_id': '1',
                 'can_notify': False,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'schema': None,
                 'additional_options': None,
                 'part_spec': None
                 })

    def test_use_customer_cluster_command_set_false(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
                    '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--retry', 3,
                     '--customer_cluster_label', 'hadoop2']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': 'hadoop2',
                 'use_customer_cluster': False,
                 'db_parallelism': None,
                 'name': None,
                 'hive_serde': None,
                 'tags': None,
                 'db_where': None,
                 'mode': '1',
                 'db_boundary_query': None,
                 'db_extract_query': None,
                 'db_split_column': None,
                 'retry': 3,
                 'command_type': 'DbImportCommand',
                 'dbtap_id': '1',
                 'can_notify': False,
                 'hive_table': 'myhivetable',
                 'db_table': 'mydbtable',
                 'schema': None,
                 'additional_options': None,
                 'part_spec': None
                 })

    def test_submit_command_with_hive_serde(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--hive_serde', 'orc', '--retry', 2]
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'customer_cluster_label': None,
                 'use_customer_cluster': False,
                 'db_parallelism': None,
                 'name': None,
                 'dbtap_id': '1',
                 'db_where': None,
                 'db_boundary_query': None,
                 'mode': '1',
                 'tags': None,
                 'command_type': 'DbImportCommand',
                 'db_split_column': None,
                 'can_notify': False,
                 'hive_table': 'myhivetable',
                 'hive_serde': 'orc',
                 'db_table': 'mydbtable',
                 'db_extract_query': None,
                 'retry': 2,
                 'schema': None,
                 'additional_options': None,
                 'part_spec': None
                 })

    def test_retry_out_of_range(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--retry', 6]
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_use_hive_partition(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--hive_serde', 'orc', '--retry', 2, '--partition_spec', 'dt=2013/country=us']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'customer_cluster_label': None,
                                                 'use_customer_cluster': False,
                                                 'db_parallelism': None,
                                                 'name': None,
                                                 'dbtap_id': '1',
                                                 'db_where': None,
                                                 'db_boundary_query': None,
                                                 'mode': '1',
                                                 'tags': None,
                                                 'command_type': 'DbImportCommand',
                                                 'db_split_column': None,
                                                 'can_notify': False,
                                                 'hive_table': 'myhivetable',
                                                 'hive_serde': 'orc',
                                                 'db_table': 'mydbtable',
                                                 'db_extract_query': None,
                                                 'retry': 2,
                                                 'schema': None,
                                                 'additional_options': None,
                                                 'part_spec': 'dt=2013/country=us'
                                                 })


class TestDbTapQueryCommand(QdsCliTestCase):

    def test_submit(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--query', 'show tables', '--db_tap_id', 1]
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'db_tap_id': 1,
                                                 'query': 'show tables',
                                                 'name': None,
                                                 'tags': None,
                                                 'macros': None,
                                                 'script_location': None,
                                                 'command_type': 'DbTapQueryCommand',
                                                 'can_notify': False})

    def test_submit_fail_with_no_parameters(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_fail_with_only_query_passed(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--query', 'show tables']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_fail_with_only_db_tap_id_passed(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--db_tap_id', 1]
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_with_no_query_or_script_location_passed(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--db_tap_id', 1, '--notify']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1})
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_with_notify(self):
         sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--query', 'show tables', '--db_tap_id', 1, '--notify']
         print_command()
         Connection._api_call = Mock(return_value={'id': 1})
         qds.main()
         Connection._api_call.assert_called_with('POST', 'commands',
                                                 {'db_tap_id': 1,
                                                  'query': 'show tables',
                                                  'tags': None,
                                                  'name': None,
                                                  'macros': None,
                                                  'script_location': None,
                                                  'command_type': 'DbTapQueryCommand',
                                                  'can_notify': True})

    def test_submit_with_name(self):
         sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--query', 'show tables', '--db_tap_id', 1, '--name', 'test_name']
         print_command()
         Connection._api_call = Mock(return_value={'id': 1})
         qds.main()
         Connection._api_call.assert_called_with('POST', 'commands',
                                                 {'db_tap_id': 1,
                                                  'query': 'show tables',
                                                  'tags': None,
                                                  'name': 'test_name',
                                                  'macros': None,
                                                  'script_location': None,
                                                  'command_type': 'DbTapQueryCommand',
                                                  'can_notify': False})

    def test_submit_with_macros(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--query', "select * from table_1 limit  \$limit\$",
                    '--db_tap_id', 1, '--macros', '[{"a": "1", "b" : "4", "limit":"a + b"}]']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'macros': [{"a": "1", "b" : "4", "limit":"a + b"}],
                                                 'db_tap_id': 1,
                                                 'query': "select * from table_1 limit  \$limit\$",
                                                 'tags': None,
                                                 'name': None,
                                                 'script_location': None,
                                                 'command_type': 'DbTapQueryCommand',
                                                 'can_notify': False})

    def test_submit_with_tags(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--query', "select * from table_1 limit  \$limit\$",
                    '--db_tap_id', 1, '--tags', 'tag1,tag2']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'macros': None,
                                                 'db_tap_id': 1,
                                                 'query': "select * from table_1 limit  \$limit\$",
                                                 'tags': ["tag1", "tag2"],
                                                 'name': None,
                                                 'script_location': None,
                                                 'command_type': 'DbTapQueryCommand',
                                                 'can_notify': False})

    def test_submit_with_s3_script_location(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--script_location', 's3://bucket/path-to-script',
                    '--db_tap_id', 1, '--tags', 'tag1,tag2']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                                                {'macros': None,
                                                 'db_tap_id': 1,
                                                 'query': None,
                                                 'script_location': 's3://bucket/path-to-script',
                                                 'tags': ["tag1", "tag2"],
                                                 'name': None,
                                                 'command_type': 'DbTapQueryCommand',
                                                 'can_notify': False})

    def test_submit_with_script_location_and_query(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit', '--query', 'show tables;','--script_location', 's3://bucket/path-to-script',
                    '--db_tap_id', 1, '--tags', 'tag1,tag2']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_with_invalid_local_script_location(self):
        sys.argv = ['qds.py', 'dbtapquerycmd', 'submit','--script_location', '/temp/bucket/path-to-script',
                    '--db_tap_id', 1, '--tags', 'tag1,tag2']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_with_valid_local_script_location(self):
        with NamedTemporaryFile() as tmp:
            tmp.write('show tables;'.encode("utf8"))
            tmp.seek(0)
            sys.argv = ['qds.py', 'dbtapquerycmd', 'submit','--script_location', tmp.name,
                        '--db_tap_id', 1, '--tags', 'tag1,tag2']
            print_command()
            Connection._api_call = Mock(return_value={'id': 1234})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'commands',
                                                    {'macros': None,
                                                     'db_tap_id': 1,
                                                     'query': 'show tables;',
                                                     'script_location': None,
                                                     'tags': ["tag1", "tag2"],
                                                     'name': None,
                                                     'command_type': 'DbTapQueryCommand',
                                                     'can_notify': False})

class TestJupyterNotebookCommand(QdsCliTestCase):

    def test_submit_none(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_no_path(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--cluster-label', 'demo-cluster']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_improper_macros(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--macros', '{"key1"}']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_improper_arguments(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--arguments', '{"key1"}']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_retry_more_than_3(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--retry', '4']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_cluster_label(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--cluster-label', 'demo-cluster']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': 'demo-cluster',
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_macros(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--macros', '[{"key1":"11","key2":"22"}, {"key3":"key1+key2"}]']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': [{"key1":"11","key2":"22"}, {"key3":"key1+key2"}],
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_arguments(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--arguments', '{"key1":"val1", "key2":"val2"}']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': '{"key1":"val1", "key2":"val2"}',
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_tags(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--tags', 'abc,def']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': ['abc', 'def'],
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_name(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--name', 'demo']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': 'demo',
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_notify(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--notify']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': True,
                 'pool': None})

    def test_submit_timeout(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--timeout', '10']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': 10,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_pool(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--pool', 'batch']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': 'batch'})

    def test_submit_no_upload_to_source(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_upload_to_source(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--upload-to-source', 'True']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_upload_to_source_false(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--upload-to-source', 'False']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': False,
                 'can_notify': False,
                 'pool': None})

    def test_submit_upload_to_source_wrong_param(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--upload-to-source', 'wrong']
        print_command()
        with self.assertRaises(qds_sdk.exception.ParseError):
            qds.main()

    def test_submit_retry(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--retry', '1']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': 1,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': None,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

    def test_submit_retry_delay(self):
        sys.argv = ['qds.py', 'jupyternotebookcmd', 'submit', '--path', 'folder/file',
                    '--retry-delay', '2']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'retry': None,
                 'name': None,
                 'tags': None,
                 'label': None,
                 'macros': None,
                 'arguments': None,
                 'timeout': None,
                 'path': 'folder/file',
                 'retry_delay': 2,
                 'command_type': 'JupyterNotebookCommand',
                 'upload_to_source': True,
                 'can_notify': False,
                 'pool': None})

class TestGetResultsCommand(QdsCliTestCase):

    def test_result_with_enable_header_true(self):
        sys.argv = ['qds.py', 'hivecmd', 'getresult', '314591', 'true']
        print_command()

        # This mock include return values of both commands/:id and commands/:id/results get calls
        Connection._api_call = Mock(return_value={'id' : 314591,
                                                  'results': '123',
                                                  'inline': True,
                                                  'qlog': "column names",
                                                  'meta_data': {'results_resource': 'commands/314591/results'},
                                                  'status': 'done'})
        qds.main()
        Connection._api_call.assert_has_calls(
            [call("GET", "commands/314591", params=None),
             call("GET", "commands/314591/results", params={'inline': True, 'include_headers': 'true'})])

    def test_result_failed_more_than_two_arguments(self):
        sys.argv = ['qds.py', 'hivecmd', 'getresult', '314591', 'true', "extra_arg"]
        print_command()

        with self.assertRaises(SystemExit):
            qds.main()


@pytest.mark.parametrize("script_location", [
    'oci://some_path/file', 'oraclebmc://some_path/file', 'wasb://some_path/file',
    'gs://some_path/file', 's3://some_path/file', 's3n://some_path/file',
    's3a://some_path/file', 'swift://some_path/file', 'adl://some_path/file',
    'abfs://some_path/file', 'abfss://some_path/file'])
def test_submit_script_location_multi_cloud(script_location):
    os.environ['QDS_API_TOKEN'] = 'dummy_token'
    os.environ['QDS_API_URL'] = 'https://qds.api.url/api'
    sys.argv = ['qds.py', 'hivecmd', 'submit', '--script_location', script_location,
                '--tags', 'abc,def']
    print_command()
    Connection._api_call = Mock(return_value={'id': 1234})
    qds.main()
    Connection._api_call.assert_called_with('POST', 'commands',
            {'macros': None,
             'hive_version': None,
             'label': None,
             'tags': ["abc", "def"],
             'sample_size': None,
             'name': None,
             'query': None,
             'command_type': 'HiveCommand',
             'can_notify': False,
             'script_location': script_location,
             'retry': 0,
             'pool': None})


if __name__ == '__main__':
    unittest.main()
