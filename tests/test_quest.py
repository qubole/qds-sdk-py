from __future__ import print_function
import sys
import os

if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import *

import qds
from qds_sdk.connection import Connection
from qds_sdk.quest import QuestCode, QuestJar
from test_base import print_command
from test_base import QdsCliTestCase

sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))


class TestQuestList(QdsCliTestCase):
    def test_list_pipeline(self):
        sys.argv = ['qds.py', 'quest', 'list', '--pipeline-status', 'draft']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'filter': "draft"}
        qds.main()
        Connection._api_call.assert_called_with("GET", "pipelines", params=params)

    def test_pause_pipeline(self):
        sys.argv = ['qds.py', 'quest', 'pause', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "pipelines/153/pause", None)

    def test_clone_pipeline(self):
        sys.argv = ['qds.py', 'quest', 'clone', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("POST", "pipelines/153/duplicate", None)

    def test_archive_pipeline(self):
        sys.argv = ['qds.py', 'quest', 'archive', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "pipelines/153/archive", None)

    def test_delete_pipeline(self):
        sys.argv = ['qds.py', 'quest', 'delete', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "pipelines/153/delete", None)

    def test_create_pipeline(self):
        sys.argv = ['qds.py', 'quest', 'create', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        d1 = {"data": {"attributes": {"name": "test_pipeline_name", "status": "DRAFT", "create_type": "3"},
                       "type": "pipelines"}}
        d2 = {"data": {"attributes": {"cluster_label": "cluster_label", "can_retry": "can_retry",
                                      "checkpoint_location": "checkpoint_location",
                                      "trigger_interval": "trigger_interval", "output_mode": "output_mode",
                                      "command_line_options": "command_line_options"}, "type": "pipeline/properties"}}
        d3 = {"data": {"attributes": {"create_type": "3", "user_arguments": "--user arguments", "code": "hello world",
                                      "language": "python"}}}
        c1 = {"req_type": "POST", "path": "pipelines?mode=wizard", "data": d1}
        c2 = {"req_type": "PUT", "path": "pipelines/1/properties", "data": d2}
        c3 = {"req_type": "PUT", "path": "pipelines/1/save_code", "data": d3}
        calls = [c1, c2, c3]
        qds.main()
        Connection._api_call.assert_has_calls(calls)
