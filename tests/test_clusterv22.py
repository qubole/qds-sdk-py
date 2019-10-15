from __future__ import print_function
import sys
import os
if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import Mock, ANY
import tempfile
sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase
from qds_sdk.cloud.cloud import Cloud
from qds_sdk.qubole import Qubole

class TestClusterCreate(QdsCliTestCase):
    def test_cluster_info(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--compute-access-key', 'aki', '--compute-secret-key', 'sak', '--min-nodes', '3',
                    '--max-nodes', '5', '--disallow-cluster-termination', '--enable-ganglia-monitoring',
                    '--node-bootstrap-file', 'test_file_name', '--master-instance-type',
                    'm1.xlarge','--slave-instance-type', 'm1.large', '--encrypted-ephemerals']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {'compute_config': {'compute_secret_key': 'sak',
                                                                                     'compute_access_key': 'aki'}},
                                                 'monitoring': {'ganglia': True},
                                                 'cluster_info': {'slave_instance_type': 'm1.large', 'min_nodes': 3,
                                                                  'max_nodes': 5, 'master_instance_type': 'm1.xlarge',
                                                                  'label': ['test_label'],
                                                                  'node_bootstrap': 'test_file_name',
                                                                  'disallow_cluster_termination': True,
                                                                  'datadisk': {'encryption': True}}})

    # default cluster
    # ondemand-ondemand-spot


