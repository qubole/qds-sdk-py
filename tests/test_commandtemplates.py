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

class TestCommandTemplate(QdsCliTestCase):

    def test_view(self):
        sys.argv = ['qds.py', 'commandtemplates', 'view', '--id', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates/123", params=None)

    def test_view_fields(self):
        sys.argv = ['qds.py', 'commandtemplates', 'view', '--id', '123', '--fields', 'id']
        print_command()
        Connection._api_call = Mock(return_value={'id':'123'})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates/123", params=None)

    def test_view_name(self):
        sys.argv = ['qds.py', 'commandtemplates', 'view', '--name', 'Show']
        print_command()
        Connection._api_call = Mock(return_value={'command_templates': ['123']})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates", params={'template_name': 'Show'})

    def test_view_no_id_no_name(self):
        sys.argv = ['qds.py', 'commandtemplates', 'view']
        print_command()
        with self.assertRaises(ParseError) as cm:
            qds.main()

    def test_list(self):
        sys.argv = ['qds.py', 'commandtemplates', 'list']
        print_command()
        Connection._api_call = Mock(return_value={'command_templates': [{'a' : 'b'}]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates", params=None)

    def test_list_fields(self):
        sys.argv = ['qds.py', 'commandtemplates', 'list', '--fields', 'a']
        print_command()
        Connection._api_call = Mock(return_value={'command_templates': [{'a' : 'b'}]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates", params=None)

    def test_list_per_page(self):
        sys.argv = ['qds.py', 'commandtemplates', 'list', '--per-page', '1']
        print_command()
        Connection._api_call = Mock(return_value={'command_templates': [{'a' : 'b'}]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates", params={'per_page': 1})

    def test_list_page_num(self):
        sys.argv = ['qds.py', 'commandtemplates', 'list', '--page', '1']
        print_command()
        Connection._api_call = Mock(return_value={'command_templates': [{'a' : 'b'}]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates", params={'page': 1})

    def test_list_page_num_per_page(self):
        sys.argv = ['qds.py', 'commandtemplates', 'list', '--page', '1', '--per-page', '2']
        print_command()
        Connection._api_call = Mock(return_value={'command_templates': [{'a' : 'b'}]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates", params={'page': 1, 'per_page' : 2})

    def test_remove(self):
        sys.argv = ['qds.py', 'commandtemplates', 'remove', '123']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = view_templates_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET", "command_templates/123", params=None),
            call("PUT", "command_templates/123/remove", {})])

    def test_remove_no_id(self):
        sys.argv = ['qds.py', 'commandtemplates', 'remove']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_run(self):
        sys.argv = ['qds.py', 'commandtemplates', 'run', '123',
         '--input_vars', "table_name='doctors'"]
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = view_templates_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET", "command_templates/123", params=None),
            call("POST", "command_templates/123/run", {'input_vars': [{'table_name':"'doctors'"}]})])

    def test_run_no_id(self):
        sys.argv = ['qds.py', 'commandtemplates', 'run']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_run_and_wait_no_id(self):
        sys.argv = ['qds.py', 'commandtemplates', 'run_and_wait']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

if __name__ == '__main__':
    unittest.main()
