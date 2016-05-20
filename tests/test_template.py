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
        Connection._api_call.side_effect = template_side_effect
        qds.main()
        data = {"command_type": "HiveCommand", "input_vars": [{"default_value": "\'age\'", "name": "col"}, 
        {"default_value": "\'user\'", "name": "table"}], "command": {"sample_size": None, "macros": None, "hive_version": None, 
        "name": None, "tags": None, "query": "select $col$ from $table$", "command_type": "HiveCommand", "can_notify": False, "script_location": None, "label": None}, "name": "tempplate_name"}
        Connection._api_call.assert_called_with("POST", "command_templates", data)
    
    def test_edit_template(self):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input_edit_template.json')
        sys.argv = ['qds.py', 'template', 'edit', '--id', '12', '--data', file_path]
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = template_side_effect
        qds.main()
        data = {"command_type": "HiveCommand", "input_vars": [{"default_value": "\'age\'", "name": "col"}, 
        {"default_value": "\'user\'", "name": "table"}], "command": {"sample_size": None, "macros": None, "hive_version": None, 
        "name": None, "tags": None, "query": "select $col$ from $table$", "command_type": "HiveCommand", "can_notify": False, "script_location": None, "label": None}, "name": "tempplate_new_name"}
        
        Connection._api_call.assert_called_with("PUT", "command_templates/12", data)
    
    def test_view_template(self):
        sys.argv = ['qds.py', 'template', 'view', '--id', '12']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = template_side_effect
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates/12", params=None)
    
    def test_list_template(self):
        sys.argv = ['qds.py', 'template', 'list', '--page', '1', '--per-page','10']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = template_side_effect
        qds.main()
        Connection._api_call.assert_called_with("GET", "command_templates?page=1&per_page=10", params=None)
    
    def test_run_template(self):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input_run_template.json')
        sys.argv = ['qds.py', 'template', 'run', '--id', '14', '--j', file_path]
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = template_side_effect
        qds.main()
        data = {'input_vars': [{'table': "'accounts'"}]}
        Connection._api_call.assert_called_with("POST", "command_templates/14/run", data)
    
    def test_run_template_with_inine_json(self):
        sys.argv = ['qds.py', 'template', 'run', '--id', '14', '--j', '[{"table" : "accounts"}]']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = template_side_effect
        qds.main()
        data = {'input_vars': [{'table': "'accounts'"}]}
        Connection._api_call.assert_called_with("POST", "command_templates/14/run", data)
        
        
        
        

if __name__ == '__main__':
    unittest.main()
        
def template_side_effect(*args, **kwargs):
    return {}
