from __future__ import print_function
import sys
import os
if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import Mock
from tempfile import NamedTemporaryFile
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

    def test_sparkcmd(self):
        sys.argv = ['qds.py', 'sparkcmd', 'check', '123']
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
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'tags': None,
                 'sample_size': None,
                 'name': None,
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
                 'tags': None,
                 'sample_size': None,
                 'name': None,
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
                 'tags': None,
                 'sample_size': None,
                 'name': None,
                 'query': None,
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script'})

    def test_submit_tags(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--script_location', 's3://bucket/path-to-script',
                    '--tags', 'abc,def']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'tags': ["abc", "def"],
                 'sample_size': None,
                 'name': None,
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
                 'tags': None,
                 'sample_size': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None})

    def test_submit_name(self):
        sys.argv = ['qds.py', 'hivecmd', 'submit', '--query', 'show tables',
                    '--name', 'test_name']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'macros': None,
                 'label': None,
                 'tags': None,
                 'sample_size': None,
                 'name': 'test_name',
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
                 'tags': None,
                 'sample_size': None,
                 'name': None,
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
                 'tags': None,
                 'sample_size': '1024',
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'HiveCommand',
                 'can_notify': False,
                 'script_location': None})

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
                 'cmdline':'/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None})

    def test_submit_script_location_aws(self):
        sys.argv = ['qds.py', 'sparkcmd', 'submit', '--script_location', 's3://bucket/path-to-script']
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
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None})

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
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None})

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
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None})

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
                     'cmdline':None,
                     'command_type': 'SparkCommand',
                     'arguments': None,
                     'user_program_arguments': None,
                     'can_notify': False,
                     'script_location': None})

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
                 'command_type': 'SparkCommand',
                 'cmdline': None,
                 'can_notify': False,
                 'script_location': None})

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
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'cmdline': None,
                 'can_notify': False,
                 'script_location': None})

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
                 'arguments': None,
                 'user_program_arguments': None,
                 'command_type': 'SparkCommand',
                 'can_notify': False,
                 'script_location': None})

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
                 'command_type': 'SparkCommand',
                 'can_notify': False,
                 'script_location': None})

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
                 'cmdline': '/usr/lib/spark/bin/spark-submit --class Test Test.jar',
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': True,
                 'script_location': None})

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
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None})

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
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': '--class HelloWorld',
                 'user_program_arguments': 'world',
                 'can_notify': False,
                 'script_location': None})

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
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
                 'can_notify': False,
                 'script_location': None})

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
                 'cmdline': None,
                 'command_type': 'SparkCommand',
                 'arguments': None,
                 'user_program_arguments': None,
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
                 'tags': None,
                 'label': None,
                 'name': None,
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
                 'tags': None,
                 'name': None,
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
                 'tags': None,
                 'label': None,
                 'name': None,
                 'query': None,
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': 's3://bucket/path-to-script'})

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
                 'tags': None,
                 'name': None,
                 'query': 'show tables',
                 'command_type': 'PrestoCommand',
                 'can_notify': False,
                 'script_location': None})

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
                 'script_location': None})

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
                 'name': None,
                 'label': None,
                 'tags': None,
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
                 'name': None,
                 'label': None,
                 'tags': None,
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
                 'name': None,
                 'label': None,
                 'tags': None,
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
                 'name': None,
                 'label': 'test_label',
                 'tags': None,
                 'command_type': 'HadoopCommand',
                 'can_notify': False})

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
                 'can_notify': False})

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
                 'can_notify': True})

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
                 'can_notify': False})


class TestShellCommand(QdsCliTestCase):

    def test_stub(self):
        pass


class TestPigCommand(QdsCliTestCase):

    def test_stub(self):
        pass


class TestDbExportCommand(QdsCliTestCase):

    def test_submit_command(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'export_dir': None,
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
                 'db_update_mode': None})

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
                {'export_dir': None,
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
                 'db_update_mode': None})

    def test_submit_with_name(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable', '--name', 'commandname']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'export_dir': None,
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
                 'db_update_mode': None})

    def test_submit_with_update_mode_and_keys(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable',
          '--db_update_mode', 'updateonly', '--db_update_keys', 'key1']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'export_dir': None,
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
                 'db_update_mode': 'updateonly'})

    def test_submit_with_mode_2(self):
        sys.argv = ['qds.py', 'dbexportcmd', 'submit', '--mode', '2', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable',
           '--export_dir', 's3:///export-path/']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'export_dir': 's3:///export-path/',
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
                 'db_update_mode': None})


class TestDbImportCommand(QdsCliTestCase):

    # Not much point adding more test cases as the semantic check in main code is still remaining.
    # The test cases might give false positivies
    def test_submit_command(self):
        sys.argv = ['qds.py', 'dbimportcmd', 'submit', '--mode', '1', '--dbtap_id', '1',
         '--db_table', 'mydbtable', '--hive_table', 'myhivetable']
        print_command()
        Connection._api_call = Mock(return_value={'id': 1234})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'commands',
                {'db_parallelism': None,
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
                 'db_table': 'mydbtable',
                 'db_extract_query': None})


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
                                                 'command_type': 'DbTapQueryCommand',
                                                 'can_notify': False})


if __name__ == '__main__':
    unittest.main()
