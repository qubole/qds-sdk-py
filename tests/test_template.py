from __future__ import print_function

import sys
import os

if sys.version_info > (2,7,0):
    import unittest
else:
    import unittest2 as unittest

from mock import *

sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds

from qds_sdk.connection import Connection
from qds_sdk.template import *
from test_base import print_command
from test_base import QdsCliTestCase

class TestTemplateCheck(QdsCliTestCase):
    def test_create_template(self):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input_create_template.json')
        sys.argv = ['qds.py', 'template', 'create', '--data', file_path]
        print_command()
        Connection._api_call = Mock()
        qds.main()
        with open(file_path) as f:
            data = json.load(f)
        Connection._api_call.assert_called_with("POST", "command_templates", data)

    def test_create_template_without_data(self):
        sys.argv = ['qds.py', 'template', 'create']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_edit_template(self):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input_edit_template.json')
        sys.argv = ['qds.py', 'template', 'edit', '--id', '12', '--data', file_path]
        print_command()
        Connection._api_call = Mock()
        qds.main()
        with open(file_path) as f:
            data = json.load(f)
        Connection._api_call.assert_called_with("PUT", "command_templates/12", data)

    def test_edit_template_without_data(self):
        sys.argv = ['qds.py', 'template', 'edit', '--id', '12']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()
    
    def test_view_template(self):
        sys.argv = ['qds.py', 'template', 'view', '--id', '12']
        print_command()
        Connection._api_call = Mock()
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates/12", params=None)
    
    def test_view_template_without_id(self):
        sys.argv = ['qds.py', 'template', 'view']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()
            
    def test_clone_template(self):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input_clone_template.json')
        sys.argv = ['qds.py', 'template', 'clone', '--id', '12', '--data', file_path]
        print_command()
        Connection._api_call = Mock()
        qds.main()
        with open(file_path) as f:
            data = json.load(f)
        Connection._api_call.assert_called_with("POST", "command_templates/12/duplicate", data)
        
    def test_clone_template_without_id_data(self):
        sys.argv = ['qds.py', 'template', 'clone']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()
    
    def test_list_template(self):
        sys.argv = ['qds.py', 'template', 'list', '--page', '1', '--per-page','10']
        print_command()
        Connection._api_call = Mock()
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates?page=1&per_page=10", params=None)
    
    def test_submit_template(self):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input_run_template.json')
        sys.argv = ['qds.py', 'template', 'submit', '--id', '14', '--j', file_path]
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = submit_actions_side_effect
        qds.main()
        with open(file_path) as f:
            data = json.load(f)
        Connection._api_call.assert_called_with("POST", "command_templates/14/run", data)
    
    def test_submit_template_without_id_data(self):
        sys.argv = ['qds.py', 'template', 'submit']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()
    
    def test_submit_template_with_inline_json(self):
        sys.argv = ['qds.py', 'template', 'submit', '--id', '14', '--j', '{"input_vars" : [{"table" : "accounts"}]}']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = submit_actions_side_effect
        qds.main()
        data = {'input_vars': [{'table': "'accounts'"}]}
        Connection._api_call.assert_called_with("POST", "command_templates/14/run", data)
    
    def test_run_template(self):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input_run_template.json')
        sys.argv = ['qds.py', 'template', 'run','--id', '14','--j',file_path]
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = submit_actions_side_effect
        HiveCommand.find = Mock()
        HiveCommand.find.side_effect = find_command_side_effect
        HiveCommand.get_results = Mock()
        qds.main()
        with open(file_path) as f:
            data = json.load(f)
        Connection._api_call.assert_called_with("POST", "command_templates/14/run", data)
                
    def test_run_template_without_id_data(self):
        sys.argv = ['qds.py', 'template', 'run']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()
            
    def test_invalid_template_operation(self):
        sys.argv = ['qds.py', 'template', 'load', '--id','12']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_delete_template_operation(self):
        sys.argv = ['qds-py', 'template', 'delete', '--id', '12']
        print_command()
        Connection._api_call = Mock()
        qds.main()
        Connection._api_call.assert_called_with("PUT", "command_templates/12/remove", None)


def submit_actions_side_effect(*args, **kwargs):
    res = {
        "id" : 122,
        "command_type" : 'HiveCommand'
    }
    return res  

def find_command_side_effect(id):
    cmd = HiveCommand()
    cmd.status = 'done'
    cmd.id = 122
    return cmd      

if __name__ == '__main__':
    unittest.main()
