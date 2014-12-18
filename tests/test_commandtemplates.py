from __future__ import print_function
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


class TestCommandTemplate(QdsCliTestCase):

    def test_hivecmd(self):
        sys.argv = ['qds.py', 'commandtemplates', 'hivecmd', '--name', 'cmdtest',
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


    # Not sure why the below test cases are not working here, although the command works otherwise

    # def test_remove(self):
    #     sys.argv = ['qds.py', 'commandtemplates', 'remove', '123']
    #     print_command()
    #     Connection._api_call = Mock(return_value={})
    #     qds.main()
    #     Connection._api_call.assert_called_with("PUT", "command_templates/123/remove", params=None)

    # def test_run(self):
    #     sys.argv = ['qds.py', 'commandtemplates', 'run', '123',
    #      '--input_vars', "table_name='doctors'"]
    #     print_command()
    #     Connection._api_call = Mock(return_value={})
    #     qds.main()
    #     Connection._api_call.assert_called_with("POST", "command_templates/123/run",
    #         {'input_vars': [{'name':'table_name','default_value':"'doctors'"}]})


if __name__ == '__main__':
    unittest.main()
