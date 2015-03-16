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
from qds_sdk.resource import BaseResource
from test_base import print_command
from test_base import QdsCliTestCase


def common_side_effect(*args, **kwargs):
    if args[1] == "actions/123":
      return {'id':'123','command': {'id':123,'command_type': 'HiveCommand','meta_data':{'results_resource':'commands/123/results'}},'status': "done"}
    elif args[1] == "commands/123/results":
      return {'results':'123','inline':True}
    else:
      return {}


class TestActionCheck(QdsCliTestCase):

    def test_list(self):
        sys.argv = ['qds.py', 'action', 'list']
        print_command()
        Connection._api_call = Mock(return_value={"actions":[]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "actions", params={})

    def test_list_pages(self):
        sys.argv = ['qds.py', 'action', 'list', '--per-page', '2']
        print_command()
        Connection._api_call = Mock(return_value={"actions":[]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "actions", params={'per_page':'2'})

    def test_list_pages_fields(self):
        sys.argv = ['qds.py', 'action', 'list', '--per-page', '2', '--fields', 'id', 'sequence_id']
        print_command()
        Connection._api_call = Mock(return_value={"actions":[]})
        qds.main()
        Connection._api_call.assert_called_with("GET", "actions", params={'per_page':'2'})

    def test_view(self):
        sys.argv = ['qds.py', 'action', 'view', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "actions/123", params=None)

    def test_view_fields(self):
        sys.argv = ['qds.py', 'action', 'view', '123', '--fields', 'id', 'sequence_id']
        print_command()
        Connection._api_call = Mock(return_value={'id':1,'sequence_id':2})
        qds.main()
        Connection._api_call.assert_called_with("GET", "actions/123", params=None)

    def test_rerun(self):
        sys.argv = ['qds.py', 'action', 'rerun', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "actions/123/rerun", None)

    def test_kill(self):
        sys.argv = ['qds.py', 'action', 'kill', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "actions/123/kill", None)

    def test_logs(self):
        sys.argv = ['qds.py', 'action', 'logs', '123']
        print_command()
        Connection._api_call = Mock(return_value={'command': {'id': 123,'command_type': 'HiveCommand','meta_data':{'logs_resource': 'commands/123/logs'}},'status': "done" })
        Connection._api_call_raw = Mock(return_value=BaseResource({'text':''}))
        qds.main()
        Connection._api_call.assert_called_with("GET", "actions/123", params=None)
        Connection._api_call_raw.assert_called_with("GET", "commands/123/logs", params=None)

    def test_results(self):
        sys.argv = ['qds.py', 'action', 'results', '123']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = common_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET", "actions/123", params=None),call("GET", "commands/123/results", params={'inline': True})])

if __name__ == '__main__':
    unittest.main()

