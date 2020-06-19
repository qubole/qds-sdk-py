from __future__ import print_function
from test_base import QdsCliTestCase
from test_base import print_command
from qds_sdk.pipelines import PipelinesCode
from qds_sdk.connection import Connection
import qds
from mock import *
import sys
import os

if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest


sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))


class TestQuestList(QdsCliTestCase):
    def test_list_pipeline(self):
        sys.argv = ['qds.py', 'pipelines', 'list', '--pipeline-status', 'draft']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'filter': "draft"}
        qds.main()
        Connection._api_call.assert_called_with(
            "GET", "pipelines", params=params)

    def test_pause_pipeline(self):
        sys.argv = ['qds.py', 'pipelines', 'pause', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "pipelines/153/pause", None)

    def test_clone_pipeline(self):
        sys.argv = ['qds.py', 'pipelines', 'clone', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "POST", "pipelines/153/duplicate", None)

    def test_archive_pipeline(self):
        sys.argv = ['qds.py', 'pipelines', 'archive', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "pipelines/153/archive", None)

    def test_delete_pipeline(self):
        sys.argv = ['qds.py', 'pipelines', 'delete', '--pipeline-id', '153']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(
            "PUT", "pipelines/153/delete", None)

    def test_create_pipeline(self):
        sys.argv = ['qds.py', 'pipelines', 'create', '--create-type', '3', '--pipeline-name', 'test_pipeline_name',
                    '--cluster-label', 'spark', '-c', 'print("hello")', '--language', 'python', '--user-arguments', 'users_argument']
        print_command()
        d1 = {"data": {"attributes": {"name": "test_pipeline_name", "status": "DRAFT", "create_type": 3},
                       "type": "pipeline"}}
        response = {"relationships": {"nodes": [], "alerts": []}, "included": [],
                    "meta": {"command_details": {"code": "print(\"hello\")", "language": "python"},
                             "properties": {"checkpoint_location": None, "trigger_interval": None,
                                            "command_line_options": """--conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native\n--conf spark.eventLog.compress=true\n--conf spark.eventLog.enabled=true\n--conf spark.sql.streaming.qubole.enableStreamingEvents=true\n--conf spark.qubole.event.enabled=true""",
                                            "cluster_label": "spark", "jar_path": None,
                                            "user_arguments": "users_argument", "main_class_name": None, "can_retry": True,
                                            "is_monitoring_enabled": True}, "query_hist": None, "cluster_id": None},
                    "data": {"id": 1, "type": "pipeline",
                             "attributes": {"name": "test_pipeline_name", "description": None, "status": "draft",
                                            "created_at": "2020-02-10T14:02:20Z", "updated_at": "2020-02-11T11:05:40Z",
                                            "cluster_label": "spark",
                                            "owner_name": "eam-airflow", "pipeline_instance_status": "draft",
                                            "create_type": 3, "health": "UNKNOWN"}}}

        PipelinesCode.pipeline_id = '1'
        PipelinesCode.pipeline_code = """print("helloworld")"""
        PipelinesCode.pipeline_name = "test_pipeline_name"
        d2 = {"data": {"attributes": {"cluster_label": "spark", "can_retry": True,
                                      "checkpoint_location": None,
                                      "trigger_interval": None, "output_mode": None,
                                      "command_line_options": """--conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native\n--conf spark.eventLog.compress=true\n--conf spark.eventLog.enabled=true\n--conf spark.sql.streaming.qubole.enableStreamingEvents=true\n--conf spark.qubole.event.enabled=true"""},
                       "type": "pipeline/properties"}}
        d3 = {"data": {
            "attributes": {"create_type": 3, "user_arguments": "users_argument", "code": """print("hello")""",
                           "language": "python"}}}
        Connection._api_call = Mock(return_value=response, any_order=False)
        qds.main()
        Connection._api_call.assert_has_calls(
            [call("POST", "pipelines?mode=wizard", d1), call("PUT", "pipelines/1/properties", d2),
             call("PUT", "pipelines/1/save_code", d3)])

if __name__ == '__main__':
    unittest.main()
