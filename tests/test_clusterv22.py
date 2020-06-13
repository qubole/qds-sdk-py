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
    # default cluster composition
    def test_cluster_info(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--compute-access-key', 'aki', '--compute-secret-key', 'sak', '--min-nodes', '3',
                    '--max-nodes', '5', '--disallow-cluster-termination', '--enable-ganglia-monitoring',
                    '--node-bootstrap-file', 'test_file_name', '--master-instance-type',
                    'm1.xlarge', '--slave-instance-type', 'm1.large', '--encrypted-ephemerals']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {
            'cloud_config': {'compute_config': {'compute_secret_key': 'sak', 'compute_access_key': 'aki'}},
            'monitoring': {'ganglia': True},
            'cluster_info': {'master_instance_type': 'm1.xlarge', 'node_bootstrap': 'test_file_name',
                             'slave_instance_type': 'm1.large', 'label': ['test_label'],
                             'disallow_cluster_termination': True, 'max_nodes': 5, 'min_nodes': 3,
                             'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                                             'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                                             'autoscaling_nodes': {'nodes': [{'percentage': 50, 'type': 'ondemand'},
                                                                             {'timeout_for_request': 1,
                                                                              'percentage': 50, 'type': 'spot',
                                                                              'fallback': 'ondemand',
                                                                              'maximum_bid_price_percentage': 100, 'allocation_strategy': None}]}},
                             'datadisk': {'encryption': True}}})

    def test_od_od_od(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'ondemand', '--min-ondemand-percentage', '100',
                    '--autoscaling-ondemand-percentage', '100']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                            'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                            'autoscaling_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]}},
            'label': ['test_label']}})

    def test_od_od_odspot(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'ondemand', '--min-ondemand-percentage', '100',
                    '--autoscaling-ondemand-percentage',
                    '50', '--autoscaling-spot-percentage', '50', '--autoscaling-maximum-bid-price-percentage', '50',
                    '--autoscaling-timeout-for-request', '3', '--autoscaling-spot-fallback', 'ondemand', '--autoscaling-spot-allocation-strategy', 'lowestPrice']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                            'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]}, 'autoscaling_nodes': {
                    'nodes': [{'percentage': 50, 'type': 'ondemand'},
                              {'timeout_for_request': 3, 'percentage': 50, 'type': 'spot', 'fallback': 'ondemand',
                               'maximum_bid_price_percentage': 50, 'allocation_strategy': 'lowestPrice'}]}}, 'label': ['test_label']}})

    def test_od_od_odspot_nofallback(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'ondemand', '--min-ondemand-percentage', '100',
                    '--autoscaling-ondemand-percentage',
                    '50', '--autoscaling-spot-percentage', '50', '--autoscaling-maximum-bid-price-percentage', '50',
                    '--autoscaling-timeout-for-request', '3', '--autoscaling-spot-fallback', None, '--autoscaling-spot-allocation-strategy', 'capacityOptimized']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                            'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]}, 'autoscaling_nodes': {
                    'nodes': [{'percentage': 50, 'type': 'ondemand'},
                              {'timeout_for_request': 3, 'percentage': 50, 'type': 'spot', 'fallback': None,
                               'maximum_bid_price_percentage': 50, 'allocation_strategy': 'capacityOptimized'}]}}, 'label': ['test_label']}})

    def test_od_od_spotblock(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'ondemand', '--min-ondemand-percentage', '100',
                    '--autoscaling-spot-block-percentage',
                    '100', '--autoscaling-spot-block-duration', '60', '--autoscaling-spot-block-fallback', 'ondemand']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                            'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                            'autoscaling_nodes': {'nodes': [{'percentage': 100, 'type': 'spotblock', 'timeout': 60, 'fallback': 'ondemand'}]}},
            'label': ['test_label']}})

    def test_od_od_spotblockspot(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'ondemand', '--min-ondemand-percentage', '100',
                    '--autoscaling-spot-block-percentage',
                    '50', '--autoscaling-spot-block-duration', '60', '--autoscaling-spot-block-fallback', None,
                    '--autoscaling-spot-percentage', '50', '--autoscaling-maximum-bid-price-percentage', '50',
                    '--autoscaling-timeout-for-request', '3', '--autoscaling-spot-fallback', None, '--autoscaling-spot-allocation-strategy', 'capacityOptimized']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                            'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]}, 'autoscaling_nodes': {
                    'nodes': [{'percentage': 50, 'type': 'spotblock', 'timeout': 60, 'fallback': None},
                              {'timeout_for_request': 3, 'percentage': 50, 'type': 'spot', 'fallback': None,
                               'maximum_bid_price_percentage': 50, 'allocation_strategy': 'capacityOptimized'}]}}, 'label': ['test_label']}})

    def test_od_od_spot(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'ondemand', '--min-ondemand-percentage', '100', '--autoscaling-spot-percentage',
                    '100',
                    '--autoscaling-maximum-bid-price-percentage', '50', '--autoscaling-timeout-for-request', '3',
                    '--autoscaling-spot-fallback', None, '--autoscaling-spot-allocation-strategy', 'lowestPrice']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                            'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]}, 'autoscaling_nodes': {
                    'nodes': [{'timeout_for_request': 3, 'percentage': 100, 'type': 'spot', 'fallback': None,
                               'maximum_bid_price_percentage': 50, 'allocation_strategy': 'lowestPrice'}]}}, 'label': ['test_label']}})

    def test_od_spot_spot(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'ondemand', '--min-spot-percentage', '100',
                    '--min-maximum-bid-price-percentage', '50', '--min-timeout-for-request', '3',
                    '--min-spot-fallback', None, '--autoscaling-spot-percentage', '100',
                    '--autoscaling-maximum-bid-price-percentage', '50', '--autoscaling-timeout-for-request', '3',
                    '--autoscaling-spot-fallback', None, '--autoscaling-spot-allocation-strategy', 'capacityOptimized', '--min-spot-allocation-strategy', 'lowestPrice']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {'composition': {'min_nodes': {
            'nodes': [{'timeout_for_request': 3, 'percentage': 100, 'type': 'spot', 'fallback': None,
                       'maximum_bid_price_percentage': 50, 'allocation_strategy': 'lowestPrice'}]}, 'master': {
            'nodes': [{'percentage': 100, 'type': 'ondemand'}]}, 'autoscaling_nodes': {'nodes': [
            {'timeout_for_request': 3, 'percentage': 100, 'type': 'spot', 'fallback': None,
             'maximum_bid_price_percentage': 50, 'allocation_strategy': 'capacityOptimized'}]}}, 'label': ['test_label']}})

    def test_spotblock_spotblock_spotblock(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'spotblock', '--master-spot-block-duration', '60', '--min-spot-block-percentage',
                    '100', '--min-spot-block-duration', '60', '--autoscaling-spot-block-percentage',
                    '100', '--autoscaling-spot-block-duration', '60', '--autoscaling-spot-block-fallback', None]
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'spotblock', 'timeout': 60}]},
                            'master': {'nodes': [{'percentage': 100, 'type': 'spotblock', 'timeout': 60}]},
                            'autoscaling_nodes': {'nodes': [{'percentage': 100, 'type': 'spotblock', 'timeout': 60, 'fallback': None}]}},
            'label': ['test_label']}})

    def test_spot_spot_spot(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--master-type', 'spot', '--master-maximum-bid-price-percentage', '50',
                    '--master-timeout-for-request', '3',
                    '--master-spot-fallback', None, '--min-spot-percentage', '100',
                    '--min-maximum-bid-price-percentage', '50', '--min-timeout-for-request', '3',
                    '--min-spot-fallback', None, '--autoscaling-spot-percentage', '100',
                    '--autoscaling-maximum-bid-price-percentage', '50', '--autoscaling-timeout-for-request', '3',
                    '--autoscaling-spot-fallback', None, '--autoscaling-spot-allocation-strategy', 'lowestPrice', '--min-spot-allocation-strategy', 'capacityOptimized']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {'composition': {'min_nodes': {
            'nodes': [{'timeout_for_request': 3, 'percentage': 100, 'type': 'spot', 'fallback': None,
                       'maximum_bid_price_percentage': 50, 'allocation_strategy': 'capacityOptimized'}]}, 'master': {'nodes': [
            {'timeout_for_request': 3, 'percentage': 100, 'type': 'spot', 'fallback': None,
             'maximum_bid_price_percentage': 50}]}, 'autoscaling_nodes': {'nodes': [
            {'timeout_for_request': 3, 'percentage': 100, 'type': 'spot', 'fallback': None,
             'maximum_bid_price_percentage': 50, 'allocation_strategy': 'lowestPrice'}]}}, 'label': ['test_label']}})

    def test_image_version_v22(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label',
                     'test_label', '--flavour', 'hive', '--slave-instance-type', 'c1.xlarge', '--min-nodes', '3', '--image-version', '1.latest']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'engine_config':
                                                     {'flavour': 'hive'},
                                                'cluster_info': {'label': ['test_label'],
                                                'min_nodes': 3,
                                                'slave_instance_type': 'c1.xlarge',
                                                'cluster_image_version': '1.latest',
                                                'composition': {'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]}, 'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]}, 'autoscaling_nodes': {'nodes': [{'percentage': 50, 'type': 'ondemand'}, {'timeout_for_request': 1, 'percentage': 50, 'type': 'spot', 'fallback': 'ondemand', 'maximum_bid_price_percentage': 100, 'allocation_strategy': None}]}}, 'label': ['test_label']}})

    def test_hive_settings(self):
        sys.argv = ['qds.py', '--version', 'v2.2', 'cluster', 'create', '--label', 'test_label',
                    '--is_hs2', 'true', '--hs2_thrift_port', '10001']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        print("Suraj")
        print(Connection._api_call)
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'engine_config': {'hive_settings': {'is_hs2': 'true', 'hs2_thrift_port': '10001'}},
                                                 'cluster_info': {'label': ['test_label'],
                                                                  'composition': {'master': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                                                                                  'min_nodes': {'nodes': [{'percentage': 100, 'type': 'ondemand'}]},
                                                                                  'autoscaling_nodes': {'nodes': [{'percentage': 50, 'type': 'ondemand'},
                                                                                                                  {'percentage': 50, 'type': 'spot', 'maximum_bid_price_percentage': 100, 'timeout_for_request': 1, 'allocation_strategy': None, 'fallback': 'ondemand'}]}}}})
class TestAzureClusterComposition(QdsCliTestCase):
    def test_od_od(self):
        sys.argv = ['qds.py', '--version', 'v2.2', '--cloud','AZURE', 'cluster',
                    'create', '--label', 'test_label',
                    '--min-ondemand-percentage', '100',
                    '--autoscaling-ondemand-percentage', '100']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {'min_nodes': {'nodes': [{'type': 'ondemand', 'percentage': 100}]},
                            'autoscaling_nodes': {'nodes': [{'type': 'ondemand', 'percentage': 100}]}},
            'label': ['test_label']}})

    def test_spot_spot(self):
        sys.argv = ['qds.py', '--version', 'v2.2', '--cloud','AZURE',  'cluster', 'create',
                    '--label', 'test_label',
                    '--min-spot-percentage', '100', '--min-spot-fallback', 'ondemand',
                    '--max-price-percentage', '50', 
                    '--autoscaling-spot-percentage', '100', "--autoscaling-spot-fallback", 'ondemand']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {
                'min_nodes': {'nodes': [{'percentage': 100, 'type': 'spot',
                                          'fallback': "ondemand", 'max_price_percentage': 50}]}, 
                'autoscaling_nodes': {'nodes': [{'percentage': 100, 'type': 'spot',
                                                 'fallback': "ondemand", 'max_price_percentage': 50}]}},
                'label': ['test_label']}})


    def test_od_spot(self):
        sys.argv = ['qds.py', '--version', 'v2.2', '--cloud','AZURE',  'cluster', 'create',
                    '--label', 'test_label',
                    '--min-ondemand-percentage', '50',
                    '--min-spot-percentage', '50',  '--min-spot-fallback', 'ondemand',
                    '--max-price-percentage', '50', 
                    '--autoscaling-ondemand-percentage', '50',
                    '--autoscaling-spot-percentage', '50', '--autoscaling-spot-fallback', 'ondemand']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cluster_info': {
            'composition': {
                'min_nodes': {'nodes': [{'type': 'ondemand', 'percentage': 50},
                                        {'percentage': 50, 'type': 'spot',
                                          'fallback': "ondemand", 'max_price_percentage': 50}]}, 
                'autoscaling_nodes': {'nodes': [{'type': 'ondemand', 'percentage': 50},
                                                {'percentage': 50, 'type': 'spot',
                                                 'fallback': "ondemand", 'max_price_percentage': 50}]}},
                'label': ['test_label']}})

if __name__ == '__main__':
    unittest.main()
