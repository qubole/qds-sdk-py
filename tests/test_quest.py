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
        sys.argv = ['qds.py', 'quest', 'create', '--create-type', '3', '--pipeline-name', 'test_pipeline_name',
                    '--cluster-label', 'spark', '-c', 'print("hello")', '--language', 'python']
        print_command()
        d1 = {"data": {"attributes": {"name": "test_pipeline_name", "status": "DRAFT", "create_type": "3"},
                       "type": "pipelines"}}
        response = {
            "relationships": {
                "nodes": [],
                "alerts": []
            },
            "included": [],
            "meta": {
                "command_details": {
                    "code": """print("hello")""",
                    "language": "python"
                },
                "properties": {
                    "checkpoint_location": None,
                    "cluster_label": "spark",
                    "is_monitoring_enabled": None
                },
                "query_hist": None,
                "cluster_id": None
            },
            "data": {
                "id": 1,
                "type": "pipeline",
                "attributes": {
                    "name": "test_pipeline_name",
                    "description": None,
                    "status": "draft",
                    "created_at": "2020-02-10T14:02:20Z",
                    "updated_at": "2020-02-10T14:02:20Z",
                    "cluster_label": None,
                    "owner_name": "eam-airflow",
                    "pipeline_instance_status": "draft",
                    "create_type": 3,
                    "health": "UNKNOWN"
                }
            }
        }
        d2 = {"data": {"attributes": {"cluster_label": "spark", "can_retry": "can_retry",
                                      "checkpoint_location": "checkpoint_location",
                                      "trigger_interval": "trigger_interval", "output_mode": "output_mode",
                                      "command_line_options": "command_line_options"}, "type": "pipeline/properties"}}
        d3 = {"data": {
            "attributes": {"create_type": "3", "user_arguments": "--user arguments", "code": """"print("hello")""",
                           "language": "python"}}}
        c1 = {"req_type": "POST", "path": "pipelines?mode=wizard", "data": d1}
        c2 = {"req_type": "PUT", "path": "pipelines/1/properties", "data": d2}
        c3 = {"req_type": "PUT", "path": "pipelines/1/save_code", "data": d3}
        calls = [call(c1), call(c2), call(c3)]
        Connection._api_call = Mock(return_value=response)
        qds.main()
        Connection._api_call.assert_has_calls(calls)
