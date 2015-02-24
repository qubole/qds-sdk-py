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
from qds_sdk.scheduler import *
from test_base import print_command
from test_base import QdsCliTestCase

def list_actions_side_effect(*args, **kwargs):
    if args[1] == "scheduler/123":
      return {'id':'123'}
    else:
      return {"actions":[]}

class TestSchedulerCheck(QdsCliTestCase):

    def test_list_actions(self):
        sys.argv = ['qds.py', 'scheduler', 'list-actions', '123']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = list_actions_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET", "scheduler/123", params=None), call("GET", "scheduler/123/actions", params={})])

    def test_list_actions_seq_id(self):
        sys.argv = ['qds.py', 'scheduler', 'list-actions', '123', '--sequence_id', '123']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = list_actions_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET", "scheduler/123",params=None), call("GET", "scheduler/123/actions/123", params={})])

    def test_list_actions_pages(self):
        sys.argv = ['qds.py', 'scheduler', 'list-actions', '123', '--per-page', '2']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = list_actions_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET", "scheduler/123",params=None), call("GET", "scheduler/123/actions", params={'per_page':'2'})])

    def test_list_actions_pages_fields(self):
        sys.argv = ['qds.py', 'scheduler', 'list-actions', '123', '--per-page', '2', '--fields', 'id', 'sequence_id']
        print_command()
        Connection._api_call = Mock()
        Connection._api_call.side_effect = list_actions_side_effect
        qds.main()
        Connection._api_call.assert_has_calls([call("GET","scheduler/123",params=None), call("GET", "scheduler/123/actions", params={'per_page':'2'})])

    def test_view_by_name(self):
        sys.argv = ['qds.py', 'scheduler', 'view_by_name', '123']
        print_command()
        Connection._api_call = Mock(return_value={"schedules":[]})
        qds.main()
        Connection._api_call.assert_has_calls([call("GET","scheduler",params={'name':'123'})])

if __name__ == '__main__':
    unittest.main()

