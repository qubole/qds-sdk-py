from __future__ import print_function
import sys
import os

if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import *

sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from qds_sdk.connection import Connection
from qds_sdk.quest import QuestCode, QuestJar
from test_base import print_command
from test_base import QdsCliTestCase


class TestQuestList(QdsCliTestCase):
    def test_list_pipeline(self):
        sys.argv = ['qds.py', 'quest', 'status', '--pipeline-status', 'all']
        print_command()
        Connection._api_call = Mock(return_value={})
        params = {'status': "all"}
        qds.main()
        Connection._api_call.assert_called_with("GET", "quest/pipelines", params=params)

