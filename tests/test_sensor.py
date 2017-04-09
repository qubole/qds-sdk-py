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
from test_base import print_command
from test_base import QdsCliTestCase


class TestSensorCheck(QdsCliTestCase):
    def test_file_sensor(self):
        sys.argv = ['qds.py', 'filesensor', 'check', '-d', '{"files":["s3://dev.canopydata.com/airflow"]}']
        print_command()
        Connection._api_call = Mock(return_value={'status': True})
        qds.main()
        Connection._api_call.assert_called_with(
            "POST", "sensors/file_sensor", {'files':['s3://dev.canopydata.com/airflow']})


    def test_partition_sensor(self):
        sys.argv = ['qds.py', 'partitionsensor', 'check', '-d', '{"schema" : "default", "table" : "nation_s3_rcfile_p", "columns" : [{"column" : "p", "values" : [1, 2]}]}']
        print_command()
        Connection._api_call = Mock(return_value={'status': True})
        qds.main()
        Connection._api_call.assert_called_with(
            "POST", "sensors/partition_sensor", {"schema" : "default", "table" : "nation_s3_rcfile_p", "columns" : [{"column" : "p", "values" : [1, 2]}]})

if __name__ == '__main__':
    unittest.main()
