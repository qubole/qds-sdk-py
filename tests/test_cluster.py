from __future__ import print_function
import sys
import os
if sys.version_info > (2, 7, 0):
    import unittest
else:
    import unittest2 as unittest
from mock import Mock
import tempfile
sys.path.append(os.path.join(os.path.dirname(__file__), '../bin'))
import qds
from qds_sdk.connection import Connection
from test_base import print_command
from test_base import QdsCliTestCase


class TestClusterList(QdsCliTestCase):

    def test_minimal(self):
        sys.argv = ['qds.py', 'cluster', 'list']
        print_command()
        Connection._api_call = Mock(return_value=[])
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", params=None)

    def test_id(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--id', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters/123", params=None)

    def test_label(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--label', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value=[])
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters/test_label", params=None)

    def test_state_up(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'up']
        print_command()
        Connection._api_call = Mock(return_value=[])
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", params=None)

    def test_state_down(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'down']
        print_command()
        Connection._api_call = Mock(return_value=[])
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", params=None)

    def test_state_pending(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'pending']
        print_command()
        Connection._api_call = Mock(return_value=[])
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", params=None)

    def test_state_terminating(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'terminating']
        print_command()
        Connection._api_call = Mock(return_value=[])
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters", params=None)

    def test_state_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'list', '--state', 'invalid']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestClusterDelete(QdsCliTestCase):
    def test_success(self):
        sys.argv = ['qds.py', 'cluster', 'delete', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("DELETE", "clusters/123", None)

    def test_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'delete']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'delete', '1', '2']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestClusterStart(QdsCliTestCase):
    def test_success(self):
        sys.argv = ['qds.py', 'cluster', 'start', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "clusters/123/state",
                {'state': 'start'})

    def test_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'start']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'start', '1', '2']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestClusterTerminate(QdsCliTestCase):
    def test_success(self):
        sys.argv = ['qds.py', 'cluster', 'terminate', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("PUT", "clusters/123/state",
                {'state': 'terminate'})

    def test_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'terminate']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'terminate', '1', '2']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestClusterStatus(QdsCliTestCase):
    def test_success(self):
        sys.argv = ['qds.py', 'cluster', 'status', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters/123/state",
                params=None)

    def test_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'status']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'status', '1', '2']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestClusterReassignLabel(QdsCliTestCase):
    def test_success(self):
        sys.argv = ['qds.py', 'cluster', 'reassign_label', '123', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT',
                'clusters/reassign-label',
                {'destination_cluster': '123', 'label': 'test_label'})

    def test_no_argument(self):
        sys.argv = ['qds.py', 'cluster', 'reassign_label']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_less_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'reassign_label', '1']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_more_arguments(self):
        sys.argv = ['qds.py', 'cluster', 'reassign_label', '1', '2', '3']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestClusterCreate(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                    }
                })

    def test_disallow_cluster_termination(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--disallow-cluster-termination']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'disallow_cluster_termination': True
                    }
                })

    def test_allow_cluster_termination(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--allow-cluster-termination']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'disallow_cluster_termination': False
                    }
                })

    def test_conflict_cluster_termination(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--allow-cluster-termination', '--disallow-cluster-termination']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_enable_ganglia_monitoring(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--enable-ganglia-monitoring']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'enable_ganglia_monitoring': True
                    }
                })

    def test_disable_ganglia_monitoring(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--disable-ganglia-monitoring']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'enable_ganglia_monitoring': False
                    }
                })

    def test_conflict_ganglia_monitoring(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--enable-ganglia-monitoring', '--disable-ganglia-monitoring']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_enable_presto(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--enable-presto']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'presto_settings': {'enable_presto': True},
                    }
                })

    def test_disable_presto(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--disable-presto']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'presto_settings': {'enable_presto': False},
                    }
                })

    @unittest.skipIf(sys.version_info < (2, 7, 0), "Known failure on Python 2.6")
    def test_conflict_presto(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--enable-presto', '--disable-presto']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_presto_custom_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                    '--access-key-id', 'aki', '--secret-access-key', 'sak',
                    '--presto-custom-config', temp.name]
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                    {'cluster':
                        {'label': ['test_label'],
                         'ec2_settings': {'compute_secret_key': 'sak',
                                          'compute_access_key': 'aki'},
                         'presto_settings':
                                {'custom_config': 'config.properties:\na=1\nb=2'}
                        }
                    })

    def test_node_bootstrap_file(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--node-bootstrap-file', 'test_file_name']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'node_bootstrap_file': 'test_file_name'
                    }
                })

    def test_aws_region_us_east_1(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki',
                                      'aws_region': 'us-east-1'},
                    }
                })

    def test_aws_region_us_west_2(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--aws-region', 'us-west-2']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki',
                                      'aws_region': 'us-west-2'},
                    }
                })

    def test_aws_region_eu_west_1(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--aws-region', 'eu-west-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki',
                                      'aws_region': 'eu-west-1'},
                    }
                })

    def test_aws_region_ap_southeast_1(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--aws-region', 'ap-southeast-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki',
                                      'aws_region': 'ap-southeast-1'},
                    }
                })

    def test_aws_region_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--aws-region', 'invalid']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_aws_availability_zone(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--aws-availability-zone', 'us-east-1a']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki',
                                      'aws_preferred_availability_zone': 'us-east-1a'},
                    }
                })

    def test_vpc_subnet(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--vpc-id', 'vpc-12345678', '--subnet-id', 'subnet-12345678']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki',
                                      'vpc_id': 'vpc-12345678',
                                      'subnet_id': 'subnet-12345678'},
                    }
                })

    def test_master_instance_type(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--master-instance-type', 'm1.xlarge']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'master_instance_type': 'm1.xlarge'}
                    }
                })

    def test_slave_instance_type(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--slave-instance-type', 'm1.large']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'slave_instance_type': 'm1.large'}
                    }
                })

    def test_initial_nodes(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--initial-nodes', '3']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'initial_nodes': 3}
                    }
                })

    def test_initial_nodes_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--initial-nodes', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_max_nodes(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--max-nodes', '5']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'max_nodes': 5}
                    }
                })

    def test_max_nodes_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--max-nodes', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_custom_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("a=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                    '--access-key-id', 'aki', '--secret-access-key', 'sak',
                    '--custom-config', temp.name]
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                    {'cluster':
                        {'label': ['test_label'],
                         'ec2_settings': {'compute_secret_key': 'sak',
                                          'compute_access_key': 'aki'},
                         'hadoop_settings':
                             {'custom_config': 'a=1\nb=2'}
                        }
                    })

    def test_custom_config_non_existent(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--custom-config', 'some_non_existent_file']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_slave_request_type_ondemand(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--slave-request-type', 'ondemand']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'slave_request_type': 'ondemand'}
                    }
                })

    def test_slave_request_type_spot(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--slave-request-type', 'spot']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'slave_request_type': 'spot'}
                    }
                })

    def test_slave_request_type_hybrid(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--slave-request-type', 'hybrid']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'slave_request_type': 'hybrid'}
                    }
                })

    def test_use_hbase(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--use-hbase']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'use_hbase': True}
                    }
                })

    def test_use_hadoop2(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--use-hadoop2']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'use_hadoop2': True}
                    }
                })

    def test_use_hadoop1(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--use-hadoop1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'use_hadoop2': False}
                    }
                })

    def test_use_spark(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--use-spark']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings': {'use_spark': True}
                    }
                })

    @unittest.skipIf(sys.version_info < (2, 7, 0), "Known failure on Python 2.6")
    def test_use_spark_on_hadoop2(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--use-spark', '--use-hadoop2']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    @unittest.skipIf(sys.version_info < (2, 7, 0), "Known failure on Python 2.6")
    def test_use_spark_on_hadoop1(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--use-spark', '--use-hadoop1']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    @unittest.skipIf(sys.version_info < (2, 7, 0), "Known failure on Python 2.6")
    def test_conflict_hadoop21_hadoop2(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--use-hadoop1', '--use-hadoop2']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_slave_request_type_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--slave-request-type', 'invalid']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_maximum_bid_price_percentage(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--maximum-bid-price-percentage', '80']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings':
                        {'spot_instance_settings':
                            {'maximum_bid_price_percentage': 80.0}
                        }
                    }
                })

    def test_maximum_bid_price_percentage_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--maximum-bid-price-percentage', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_timeout_for_spot_request(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--timeout-for-spot-request', '3']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings':
                        {'spot_instance_settings':
                            {'timeout_for_request': 3}
                        }
                    }
                })

    def test_timeout_for_spot_request_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--timeout-for-spot-request', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_maximum_spot_instance_percentage(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--maximum-spot-instance-percentage', '40']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings':
                        {'spot_instance_settings':
                            {'maximum_spot_instance_percentage': 40}
                        }
                    }
                })

    def test_maximum_spot_instance_percentage_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--maximum-spot-instance-percentage', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_encrypted_ephemerals(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--encrypted-ephemerals']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'security_settings': {'encrypted_ephemerals': True},
                    }
                })

    def test_no_encrypted_ephemerals(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--no-encrypted-ephemerals']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'security_settings': {'encrypted_ephemerals': False},
                    }
                })

    @unittest.skipIf(sys.version_info < (2, 7, 0), "Known failure on Python 2.6")
    def test_conflict_encrypted_ephemerals(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--encrypted-ephemerals', '--no-encrypted-ephemerals']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


    def test_customer_ssh_key(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("ssh-rsa Blah1/Blah2+BLAH3==".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                    '--access-key-id', 'aki', '--secret-access-key', 'sak',
                    '--customer-ssh-key', temp.name]
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                    {'cluster':
                        {'label': ['test_label'],
                         'ec2_settings': {'compute_secret_key': 'sak',
                                          'compute_access_key': 'aki'},
                         'security_settings': {'customer_ssh_key': 'ssh-rsa Blah1/Blah2+BLAH3=='},
                        }
                    })

    def test_customer_ssh_key_non_existent(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--customer-ssh-key', 'some_non_existent_file']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fairscheduler_config_xml(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("<tag>60</tag>".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                    '--access-key-id', 'aki', '--secret-access-key', 'sak',
                    '--fairscheduler-config-xml', temp.name]
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                    {'cluster':
                        {'label': ['test_label'],
                         'ec2_settings': {'compute_secret_key': 'sak',
                                          'compute_access_key': 'aki'},
                         'hadoop_settings':
                            {'fairscheduler_settings':
                                {'fairscheduler_config_xml': '<tag>60</tag>'}
                            }
                        }
                    })

    def test_fairscheduler_config_xml_non_existent(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--fairscheduler-config-xml', 'some_non_existent_file']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fairscheduler_default_pool(self):
        sys.argv = ['qds.py', 'cluster', 'create', '--label', 'test_label',
                '--access-key-id', 'aki', '--secret-access-key', 'sak',
                '--fairscheduler-default-pool', 'test_pool']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                {'cluster':
                    {'label': ['test_label'],
                     'ec2_settings': {'compute_secret_key': 'sak',
                                      'compute_access_key': 'aki'},
                     'hadoop_settings':
                        {'fairscheduler_settings':
                            {'default_pool': 'test_pool'}
                        }
                    }
                })


class TestClusterUpdate(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123', {})

    def test_label(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--label', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'label': ['test_label']
                    }
                })

    def test_access_key_id(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--access-key-id', 'aki']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'compute_access_key': 'aki'}
                    }
                })

    def test_secret_access_key(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--secret-access-key', 'sak']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'compute_secret_key': 'sak'}
                    }
                })

    def test_disallow_cluster_termination(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--disallow-cluster-termination']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'disallow_cluster_termination': True
                    }
                })

    def test_allow_cluster_termination(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--allow-cluster-termination']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'disallow_cluster_termination': False
                    }
                })

    def test_conflict_cluster_termination(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--allow-cluster-termination', '--disallow-cluster-termination']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_enable_ganglia_monitoring(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--enable-ganglia-monitoring']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'enable_ganglia_monitoring': True
                    }
                })

    def test_disable_ganglia_monitoring(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--disable-ganglia-monitoring']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'enable_ganglia_monitoring': False
                    }
                })

    def test_conflict_ganglia_monitoring(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--enable-ganglia-monitoring', '--disable-ganglia-monitoring']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_enable_presto(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--enable-presto']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'presto_settings': {'enable_presto': True},
                    }
                })

    def test_disable_presto(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--disable-presto']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'presto_settings': {'enable_presto': False},
                    }
                })

    @unittest.skipIf(sys.version_info < (2, 7, 0), "Known failure on Python 2.6")
    def test_conflict_presto(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--enable-presto', '--disable-presto']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_presto_custom_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', 'cluster', 'update', '123',
                    '--presto-custom-config', temp.name]
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('PUT', 'clusters/123',
                    {'cluster':
                        {
                         'presto_settings':
                                {'custom_config': 'config.properties:\na=1\nb=2'}
                        }
                    })

    def test_node_bootstrap_file(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--node-bootstrap-file', 'test_file_name']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'node_bootstrap_file': 'test_file_name'
                    }
                })

    def test_aws_region_us_east_1(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--aws-region', 'us-east-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'aws_region': 'us-east-1'},
                    }
                })

    def test_aws_region_us_west_2(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--aws-region', 'us-west-2']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'aws_region': 'us-west-2'},
                    }
                })

    def test_aws_region_eu_west_1(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--aws-region', 'eu-west-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'aws_region': 'eu-west-1'},
                    }
                })

    def test_aws_region_ap_southeast_1(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--aws-region', 'ap-southeast-1']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'aws_region': 'ap-southeast-1'},
                    }
                })

    def test_aws_region_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--aws-region', 'invalid']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_aws_availability_zone(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--aws-availability-zone', 'us-east-1a']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'aws_preferred_availability_zone': 'us-east-1a'},
                    }
                })

    def test_vpc(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--vpc-id', 'vpc-12345678']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'vpc_id': 'vpc-12345678'},
                    }
                })

    def test_subnet(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--subnet-id', 'subnet-12345678']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'ec2_settings': {'subnet_id': 'subnet-12345678'},
                    }
                })

    def test_master_instance_type(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--master-instance-type', 'm1.xlarge']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings': {'master_instance_type': 'm1.xlarge'}
                    }
                })

    def test_slave_instance_type(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--slave-instance-type', 'm1.large']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings': {'slave_instance_type': 'm1.large'}
                    }
                })

    def test_initial_nodes(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--initial-nodes', '3']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings': {'initial_nodes': 3}
                    }
                })

    def test_initial_nodes_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--initial-nodes', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_max_nodes(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--max-nodes', '5']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings': {'max_nodes': 5}
                    }
                })

    def test_max_nodes_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--max-nodes', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_custom_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("a=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', 'cluster', 'update', '123',
                    '--custom-config', temp.name]
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('PUT', 'clusters/123',
                    {'cluster':
                        {
                         'hadoop_settings':
                             {'custom_config': 'a=1\nb=2'}
                        }
                    })

    def test_custom_config_non_existent(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--custom-config', 'some_non_existent_file']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_slave_request_type_ondemand(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--slave-request-type', 'ondemand']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings': {'slave_request_type': 'ondemand'}
                    }
                })

    def test_slave_request_type_spot(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--slave-request-type', 'spot']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings': {'slave_request_type': 'spot'}
                    }
                })

    def test_slave_request_type_hybrid(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--slave-request-type', 'hybrid']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings': {'slave_request_type': 'hybrid'}
                    }
                })

    def test_slave_request_type_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--slave-request-type', 'invalid']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_maximum_bid_price_percentage(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--maximum-bid-price-percentage', '80']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings':
                        {'spot_instance_settings':
                            {'maximum_bid_price_percentage': 80.0}
                        }
                    }
                })

    def test_maximum_bid_price_percentage_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--maximum-bid-price-percentage', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_timeout_for_spot_request(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--timeout-for-spot-request', '3']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings':
                        {'spot_instance_settings':
                            {'timeout_for_request': 3}
                        }
                    }
                })

    def test_timeout_for_spot_request_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--timeout-for-spot-request', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_maximum_spot_instance_percentage(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--maximum-spot-instance-percentage', '40']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings':
                        {'spot_instance_settings':
                            {'maximum_spot_instance_percentage': 40}
                        }
                    }
                })

    def test_maximum_spot_instance_percentage_invalid(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--maximum-spot-instance-percentage', 'not_number']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_encrypted_ephemerals(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--encrypted-ephemerals']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'security_settings': {'encrypted_ephemerals': True},
                    }
                })

    def test_no_encrypted_ephemerals(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--no-encrypted-ephemerals']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'security_settings': {'encrypted_ephemerals': False},
                    }
                })

    @unittest.skipIf(sys.version_info < (2, 7, 0), "Known failure on Python 2.6")
    def test_conflict_encrypted_ephemerals(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--encrypted-ephemerals', '--no-encrypted-ephemerals']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


    def test_customer_ssh_key(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("ssh-rsa Blah1/Blah2+BLAH3==".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', 'cluster', 'update', '123',
                    '--customer-ssh-key', temp.name]
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('PUT', 'clusters/123',
                    {'cluster':
                        {
                         'security_settings': {'customer_ssh_key': 'ssh-rsa Blah1/Blah2+BLAH3=='},
                        }
                    })

    def test_customer_ssh_key_non_existent(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--customer-ssh-key', 'some_non_existent_file']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fairscheduler_config_xml(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("<tag>60</tag>".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', 'cluster', 'update', '123',
                    '--fairscheduler-config-xml', temp.name]
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('PUT', 'clusters/123',
                    {'cluster':
                        {
                         'hadoop_settings':
                            {'fairscheduler_settings':
                                {'fairscheduler_config_xml': '<tag>60</tag>'}
                            }
                        }
                    })

    def test_fairscheduler_config_xml_non_existent(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--fairscheduler-config-xml', 'some_non_existent_file']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_fairscheduler_default_pool(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                '--fairscheduler-default-pool', 'test_pool']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                {'cluster':
                    {
                     'hadoop_settings':
                        {'fairscheduler_settings':
                            {'default_pool': 'test_pool'}
                        }
                    }
                })

    def test_custom_ec2_tags(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                    '--custom-ec2-tags', '{"foo":"bar", "bar":"baz"}']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cluster': {
                                                    'hadoop_settings': {
                                                        "custom_ec2_tags": {
                                                            "foo": "bar",
                                                            "bar": "baz"
                                                        }
                                                    }
                                                }})

    def test_persistent_security_group(self):
        sys.argv = ['qds.py', 'cluster', 'update', '123',
                    '--persistent-security-group', 'foopsg']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cluster': {
                                                    'security_settings': {
                                                        "persistent_security_group": "foopsg"
                                                    }
                                                }})


class TestClusterClone(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', 'cluster', 'clone', '1234', '--label', 'test_label1', 'test_label2' ]
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/clone',
                                                {
                                                    "cluster": {
                                                        "label": [
                                                            "test_label1",
                                                            "test_label2"
                                                        ]
                                                    }
                                                })

class TestClusterHbaseSnapshot(QdsCliTestCase):
    def test_snapshot(self):
        sys.argv = ['qds.py', 'cluster', 'snapshot', '--label', '1234', '--s3_location', 'myString', '--backup_type', 'full']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/snapshots', {'s3_location':'myString', 'backup_type':'full'})

    def test_snapshot_with_no_label(self):
        sys.argv = ['qds.py', 'cluster', 'snapshot', '--s3_location', 'myString', '--backup_type', 'full']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_snapshot_with_no_s3_location(self):
        sys.argv = ['qds.py', 'cluster', 'snapshot', '--label', '1234', '--backup_type', 'full']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_snapshot_with_no_backup_type(self):
        sys.argv = ['qds.py', 'cluster', 'snapshot', '--label', '1234', '--s3_location', 'myString']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/snapshots', {'s3_location':'myString'})

    def test_restore_point(self):
        sys.argv = ['qds.py', 'cluster', 'restore_point', '--label', '1234', '--s3_location', 'myString', '--backup_id', 'abcd', '--table_names', 'tablename']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/restore_point', {'s3_location':'myString', 'backup_id':'abcd', 'table_names':'tablename', 'automatic': True, 'overwrite': True})

    def test_restore_point_no_overwrite(self):
        sys.argv = ['qds.py', 'cluster', 'restore_point', '--label', '1234', '--s3_location', 'myString', '--backup_id', 'abcd', '--table_names', 'tablename', '--no-overwrite']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/restore_point', {'s3_location':'myString', 'backup_id':'abcd', 'table_names':'tablename', 'automatic': True, 'overwrite': False})

    def test_restore_point_no_automatic(self):
        sys.argv = ['qds.py', 'cluster', 'restore_point', '--label', '1234', '--s3_location', 'myString', '--backup_id', 'abcd', '--table_names', 'tablename', '--no-automatic']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/restore_point', {'s3_location':'myString', 'backup_id':'abcd', 'table_names':'tablename', 'automatic': False, 'overwrite': True})

    def test_restore_point_no_overwrite_and_no_automatic(self):
        sys.argv = ['qds.py', 'cluster', 'restore_point', '--label', '1234', '--s3_location', 'myString', '--backup_id', 'abcd', '--table_names', 'tablename', '--no-overwrite', '--no-automatic']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/restore_point', {'s3_location':'myString', 'backup_id':'abcd', 'table_names':'tablename', 'automatic': False, 'overwrite': False})

    def test_restore_point_with_no_label(self):
        sys.argv = ['qds.py', 'cluster', 'restore_point', '--s3_location', 'myString', '--backup_id', 'abcd', '--table_names', 'tablename']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_restore_point_with_no_s3_location(self):
        sys.argv = ['qds.py', 'cluster', 'restore_point', '--label', '1234', '--backup_id', 'abcd', '--table_names', 'tablename']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_restore_point_with_no_backup_id(self):
        sys.argv = ['qds.py', 'cluster', 'restore_point', '--label', '1234','--s3_location', 'myString', '--table_names', 'tablename']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_restore_point_with_no_table_names(self):
        sys.argv = ['qds.py', 'cluster', 'restore_point', '--label', '1234','--s3_location', 'myString', '--backup_id', 'abcd']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_snapshot_schedule_with_suspended(self):
        sys.argv = ['qds.py', 'cluster', 'update_snapshot_schedule', '--label', '1234', '--status', 'SUSPENDED']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/1234/snapshot_schedule', {"status":"SUSPENDED"})

    def test_snapshot_schedule_with_running(self):
        sys.argv = ['qds.py', 'cluster', 'update_snapshot_schedule', '--label', '1234', '--status', 'RUNNING']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/1234/snapshot_schedule', {"status":"RUNNING"})
    
    def test_update_snapshot_schedule_with_no_label(self):
        sys.argv = ['qds.py', 'cluster', 'update_snapshot_schedule', '--status', 'SUSPENDED']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_snapshot_schedule_with_kill(self):
        sys.argv = ['qds.py', 'cluster', 'update_snapshot_schedule', '--label', '1234', '--status', 'KILL']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_get_snapshot_schedule_with_no_label(self):
        sys.argv = ['qds.py', 'cluster', 'get_snapshot_schedule']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_get_snapshot_schedule(self):
        sys.argv = ['qds.py', 'cluster', 'get_snapshot_schedule', '--label', '1234']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters/1234/snapshot_schedule', params=None)
    
    def test_snapshot_schedule_with_s3_location(self):
        sys.argv = ['qds.py', 'cluster', 'update_snapshot_schedule', '--label', '1234', '--s3-location', 'mysite.com']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/1234/snapshot_schedule', {"s3_location":"mysite.com"})
    
    def test_update_snapshot_schedule(self):
        sys.argv = ['qds.py', 'cluster', 'update_snapshot_schedule', '--label', '1234', '--s3-location', 'mysite.com', '--frequency-unit', 'days', '--frequency-num', '30']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/1234/snapshot_schedule', {"s3_location":"mysite.com", "frequency_num":"30", "frequency_unit":"days"})
    
class TestClusterManageCommands(QdsCliTestCase):
    def test_add_command(self):
        sys.argv = ['qds.py', 'cluster', 'add_node', '--id', '1234']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/nodes', {'parameters' : {}})

    def test_add_node_label_id_exclusivity(self):
        sys.argv = ['qds.py', 'cluster', 'add_node', '--id', '1234', '--label', 'dummy_label']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_add_node_with_private_dns(self):
        sys.argv = ['qds.py', 'cluster', 'add_node', '--id', '1234', '--private_dns', 'test_dns']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_replace_command(self):
        sys.argv = ['qds.py', 'cluster', 'update_node', '--id', '1234', '--command', 'replace','--private_dns', 'test_private_dns']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/1234/nodes', {'parameters' : {}, 'private_dns' : "test_private_dns", 'command' : "replace"})

    def test_update(self):
        sys.argv = ['qds.py', 'cluster', 'update_node', '--id', '1234', '--command', 'replace']
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

    def test_remove_command(self):
        sys.argv = ['qds.py', 'cluster', 'remove_node', '--id', '1234', '--private_dns', 'test_private_dns']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with(r'DELETE', 'clusters/1234/nodes', {'parameters' : {}, 'private_dns' : "test_private_dns"})

    def test_update(self):
        sys.argv = ['qds.py', 'cluster', 'remove_node', '--id', '1234' ]
        print_command()
        Connection._api_call = Mock(return_value={})
        with self.assertRaises(SystemExit):
            qds.main()

if __name__ == '__main__':
    unittest.main()
