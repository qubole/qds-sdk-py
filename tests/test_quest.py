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

    def test_pipeline_status(self):
        sys.argv = ['qds.py', 'quest', 'status', '--pipeline-id', '1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "pipelines/1", None)