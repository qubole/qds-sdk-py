from __future__ import print_function
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
from test_base import print_command
from test_base import QdsCliTestCase
from argparse import ArgumentError
from qds_sdk.exception import *
def view_templates_side_effect(*args, **kwargs):
    if args[1] == "command_templates/123":
      return {'id':'123'}
    else:
      return {"actions":[]}

class TestHiveCommandTemplate(QdsCliTestCase):
    
    def test_create_no_name(self):
        sys.argv = ['qds.py', 'commandtemplates', 'hivecmd',
         '--query', 'select * from \$table_name\$', '--input_vars', "table_name='doctors'"]
        print_command()
        with self.assertRaises(SystemExit) as cm:
            qds.main()

        self.assertEqual(cm.exception.code, 2)
    
    def test_create_no_query_no_script_location(self):
        sys.argv = ['qds.py', 'commandtemplates', 'hivecmd',
         '--name', 'no_query_no_script_location']
        print_command()
        with self.assertRaises(ParseError) as cm:
            qds.main()

    def test_create_both_query_script_location(self):
        sys.argv = ['qds.py', 'commandtemplates', 'hivecmd',
         '--name', 'both_query_script_location','--query', 'show tables', '--script_location', 'random location']
        print_command()
        with self.assertRaises(SystemExit) as cm:
            qds.main()
    
        self.assertEqual(cm.exception.code, 2)

class TestPrestoCommandTemplate(QdsCliTestCase):
    
    def test_create_no_name(self):
        sys.argv = ['qds.py', 'commandtemplates', 'prestocmd',
         '--query', 'select * from \$table_name\$', '--input_vars', "table_name='doctors'"]
        print_command()
        with self.assertRaises(SystemExit) as cm:
            qds.main()

        self.assertEqual(cm.exception.code, 2)
    
    def test_create_no_query_no_script_location(self):
        sys.argv = ['qds.py', 'commandtemplates', 'prestocmd',
         '--name', 'no_query_no_script_location']
        print_command()
        with self.assertRaises(ParseError) as cm:
            qds.main()

    def test_create_both_query_script_location(self):
        sys.argv = ['qds.py', 'commandtemplates', 'prestocmd',
         '--name', 'both_query_script_location','--query', 'show tables', '--script_location', 'random location']
        print_command()
        with self.assertRaises(SystemExit) as cm:
            qds.main()
    
        self.assertEqual(cm.exception.code, 2)


class TestCommandTemplate(QdsCliTestCase):

    def test_hivecmd(self):
        sys.argv = ['qds.py', 'commandtemplates', 'hivecmd','--name', 'cmdtest',
         '--query', 'select * from \$table_name\$', '--input_vars', "table_name='doctors'"]
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "command_templates", 
            {'name': 'cmdtest',
             'input_vars': [{'name':'table_name','default_value':"'doctors'"}],
             'command_type':'HiveCommand',
             'command':{'query':'select * from \$table_name\$','command_type':'HiveCommand'}
            })

    def test_view(self):
        sys.argv = ['qds.py', 'commandtemplates', 'view', '--id', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates/123", params=None)


    def test_remove(self):
        sys.argv = ['qds.py', 'commandtemplates', 'remove', '123']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = view_templates_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET", "command_templates/123", params=None),
            call("PUT", "command_templates/123/remove", {})])

    def test_run(self):
        sys.argv = ['qds.py', 'commandtemplates', 'run', '123',
         '--input_vars', "table_name='doctors'"]
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = view_templates_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET", "command_templates/123", params=None),
            call("POST", "command_templates/123/run", {'input_vars': [{'table_name':"'doctors'"}]})])



if __name__ == '__main__':
    unittest.main()
