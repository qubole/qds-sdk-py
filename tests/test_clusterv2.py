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
    def test_minimal(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--compute-access-key', 'aki', '--compute-secret-key', 'sak']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info':
                                                     {'label': ['test_label']
                                                      },
                                                 'cloud_config': {
                                                     'compute_config': {
                                                         'compute_secret_key': 'sak',
                                                         'compute_access_key': 'aki'}}
                                                 })

    def test_cluster_info(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--compute-access-key', 'aki', '--compute-secret-key', 'sak', '--min-nodes', '3',
                    '--max-nodes', '5', '--disallow-cluster-termination', '--enable-ganglia-monitoring',
                    '--node-bootstrap-file', 'test_file_name', '--master-instance-type',
                    'm1.xlarge', '--slave-instance-type', 'm1.large', '--encrypted-ephemerals']
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

    def test_aws_compute_config(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--enable-account-compute-creds']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {
                                                    'compute_config': {'use_account_compute_creds': True}},
                                                    'cluster_info': {'label': ['test_label']}})

    def test_aws_network_config(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--enable-account-compute-creds', '--vpc-id', 'vpc-12345678', '--subnet-id', 'subnet-12345678',
                    '--bastion-node-public-dns', 'dummydns', '--persistent-security-groups',
                    'foopsg', '--master-elastic-ip', "10.10.10.10"]
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {'compute_config': {'use_account_compute_creds': True},
                                                                  'network_config': {'subnet_id': 'subnet-12345678',
                                                                                     'vpc_id': 'vpc-12345678',
                                                                                     'master_elastic_ip': '10.10.10.10',
                                                                                     'persistent_security_groups': 'foopsg',
                                                                                     'bastion_node_public_dns': 'dummydns'}},
                                                 'cluster_info': {'label': ['test_label']}})

    def test_aws_location_config(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--aws-region', 'us-east-1', '--aws-availability-zone', 'us-east-1a']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config': {'location': {
            'aws_availability_zone': 'us-east-1a',
            'aws_region': 'us-east-1'}},
            'cluster_info': {'label': ['test_label']}})

    def test_oracle_bmc_compute_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'create', '--label', 'test_label',
                    '--compute-tenant-id', 'xxx11', '--compute-user-id', 'yyyy11', '--compute-key-finger-print',
                    'zzz22', '--compute-api-private-rsa-key', 'aaa']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config': {'compute_config':
                                                                                          {
                                                                                              'compute_key_finger_print': 'zzz22',
                                                                                              'compute_api_private_rsa_key': 'aaa',
                                                                                              'compute_user_id': 'yyyy11',
                                                                                              'compute_tenant_id': 'xxx11'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_oracle_bmc_storage_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'create', '--label', 'test_label',
                    '--storage-tenant-id', 'xxx11', '--storage-user-id', 'yyyy11', '--storage-key-finger-print',
                    'zzz22', '--storage-api-private-rsa-key', 'aaa', '--block-volume-count', '1',
                    '--block-volume-size', '100']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config':
                                                                         {'storage_config':
                                                                              {'storage_key_finger_print': 'zzz22',
                                                                               'storage_api_private_rsa_key': 'aaa',
                                                                               'storage_user_id': 'yyyy11',
                                                                               'storage_tenant_id': 'xxx11',
                                                                               'block_volume_count': 1,
                                                                               'block_volume_size': 100}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_oracle_bmc_network_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'create', '--label', 'test_label',
                    '--compartment-id', 'abc-compartment', '--image-id', 'abc-image', '--vcn-id', 'vcn-1',
                    '--subnet-id', 'subnet-1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config': {'network_config':
                                                                                          {'subnet_id': 'subnet-1',
                                                                                           'vcn_id': 'vcn-1',
                                                                                           'compartment_id': 'abc-compartment',
                                                                                           'image_id': 'abc-image'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_oracle_bmc_network_config_az_info_map(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'create', '--label', 'test_label',
                    '--compartment-id', 'abc-compartment', '--image-id', 'abc-image', '--vcn-id', 'vcn-1',
                    '--availability-domain-info-map', str([{"availability_domain": "AD-1", "subnet_id": "subnet-1"}])]
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config':
                                                                         {'network_config':
                                                                              {'vcn_id': 'vcn-1',
                                                                               'compartment_id': 'abc-compartment',
                                                                               'image_id': 'abc-image',
                                                                               'availability_domain_info_map':
                                                                                   [{'availability_domain': 'AD-1',
                                                                                     'subnet_id': 'subnet-1'}]}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_oracle_bmc_location_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'create', '--label', 'test_label',
                    '--oracle-region', 'us-phoenix-1', '--oracle-availability-zone', 'phx-ad-1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config': {'location':
                                                                                          {'region': 'us-phoenix-1',
                                                                                           'availability_domain': 'phx-ad-1'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_azure_compute_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'create', '--label', 'test_label',
                    '--compute-client-id', 'testclientid', '--compute-client-secret', 'testclientsecret',
                    '--compute-tenant-id', 'testtenantid', '--compute-subscription-id', 'testsubscriptionid',
                    '--disable-account-compute-creds']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {
                                                    'compute_config': {'compute_subscription_id': 'testsubscriptionid',
                                                                       'compute_client_id': 'testclientid',
                                                                       'compute_client_secret': 'testclientsecret',
                                                                       'use_account_compute_creds': 'False',
                                                                       'compute_tenant_id': 'testtenantid',
                                                                       'use_account_compute_creds': False}},
                                                    'cluster_info': {'label': ['test_label']}})

    def test_azure_storage_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'create', '--label', 'test_label',
                    '--storage-access-key', 'testkey', '--storage-account-name', 'test_account_name',
                    '--disk-storage-account-name', 'testaccname', '--disk-storage-account-resource-group-name',
                    'testgrpname']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info':
                                                     {'label': ['test_label']},
                                                 'cloud_config':
                                                     {'storage_config':
                                                          {'storage_access_key': 'testkey',
                                                           'storage_account_name': 'test_account_name',
                                                           'disk_storage_account_name': 'testaccname',
                                                           'disk_storage_account_resource_group_name': 'testgrpname'}
                                                      }
                                                 })

    def test_azure_managed_disk_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'create', '--label', 'test_label',
                    '--storage-access-key', 'testkey', '--storage-account-name', 'test_account_name',
                    '--managed-disk-account-type', 'test_managed_disk']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info':
                                                     {'label': ['test_label']},
                                                 'cloud_config':
                                                     {'storage_config':
                                                          {'storage_access_key': 'testkey',
                                                           'storage_account_name': 'test_account_name',
                                                           'managed_disk_account_type': 'test_managed_disk'
                                                           }
                                                      }
                                                 })

    def test_azure_network_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'create', '--label', 'test_label',
                    '--vnet-name', 'testvnet', '--subnet-name', 'testsubnet',
                    '--vnet-resource-group-name', 'vnetresname']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {
                                                    'network_config': {'vnet_resource_group_name': 'vnetresname',
                                                                       'subnet_name': 'testsubnet',
                                                                       'vnet_name': 'testvnet'}},
                                                    'cluster_info': {'label': ['test_label']}})

    def test_azure_master_static_nic(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'create', '--label', 'test_label',
                    '--vnet-name', 'testvnet', '--subnet-name', 'testsubnet',
                    '--vnet-resource-group-name', 'vnetresname', '--master-static-nic-name', 'nic1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {
                                                    'network_config': {'vnet_resource_group_name': 'vnetresname',
                                                                       'subnet_name': 'testsubnet',
                                                                       'vnet_name': 'testvnet',
                                                                       'master_static_nic_name': 'nic1'}},
                                                    'cluster_info': {'label': ['test_label']}})

    def test_azure_master_static_pip(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'create', '--label', 'test_label',
                    '--vnet-name', 'testvnet', '--subnet-name', 'testsubnet',
                    '--vnet-resource-group-name', 'vnetresname', '--master-static-public-ip-name', 'pip1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {
                                                    'network_config': {'vnet_resource_group_name': 'vnetresname',
                                                                       'subnet_name': 'testsubnet',
                                                                       'vnet_name': 'testvnet',
                                                                       'master_static_public_ip_name': 'pip1'}},
                                                    'cluster_info': {'label': ['test_label']}})

    def test_azure_resource_group_name(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'create', '--label', 'test_label',
                    '--resource-group-name', 'testrg']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {
                                                    'resource_group_name': 'testrg'
                                                },
                                                    'cluster_info': {'label': ['test_label']}})

    def test_oracle_opc_compute_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_OPC', 'cluster', 'create', '--label', 'test_label',
                    '--username', 'testusername', '--password', 'testpassword',
                    '--rest-api-endpoint', 'testrestapiendpoint', '--disable-account-compute-creds']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {
                                                    'compute_config': {'username': 'testusername',
                                                                       'password': 'testpassword',
                                                                       'rest_api_endpoint': 'testrestapiendpoint',
                                                                       'use_account_compute_creds': False}},
                                                    'cluster_info': {'label': ['test_label']}})

    def test_oracle_opc_storage_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_OPC', 'cluster', 'create', '--label', 'test_label',
                    '--storage-username', 'testusername', '--storage-password', 'testpassword',
                    '--storage-rest-api-endpoint', 'testrestapiendpoint', '--count', '1', '--size', '100']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info':
                                                     {'label': ['test_label'],
                                                      'datadisk': {'count': 1, 'size': 100}
                                                      },
                                                 'cloud_config':
                                                     {'storage_config':
                                                          {'storage_username': 'testusername',
                                                           'storage_password': 'testpassword',
                                                           'storage_rest_api_endpoint': 'testrestapiendpoint',
                                                           'data_disk_count': 1,
                                                           'data_disk_size': 100}
                                                      }
                                                 })

    def test_oracle_opc_network_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_OPC', 'cluster', 'create', '--label', 'test_label',
                    '--acl', 'testacl', '--ip-network', 'testipnetwork']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cloud_config': {
                                                    'network_config': {'acl': 'testacl',
                                                                       'ip_network': 'testipnetwork'}},
                                                    'cluster_info': {'label': ['test_label']}})

    def test_gcp_compute_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'GCP', 'cluster', 'create', '--label', 'test_label',
                    '--qsa-client-id', 'xxx11', '--customer-project-id', 'www11', '--qsa-client-email',
                    'yyyy11', '--qsa-private-key-id', 'zzz22', '--qsa-private-key', 'aaa']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config': {'compute_config':
                                                                                          {
                                                                                              'qsa_private_key_id': 'zzz22',
                                                                                              'qsa_private_key': 'aaa',
                                                                                              'qsa_client_email': 'yyyy11',
                                                                                              'customer_project_id': 'www11',
                                                                                              'qsa_client_id': 'xxx11'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_gcp_storage_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'GCP', 'cluster', 'create', '--label', 'test_label',
                    '--storage-client-email', 'aaa', '--storage-disk-size-in-gb', 'aaa',
                    '--storage-disk-count', 'bbb', '--storage-disk-type', 'ccc']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config':
                                                                         {'storage_config':
                                                                              {'inst_client_email': 'aaa',
                                                                               'disk_size_in_gb': 'aaa',
                                                                               'disk_count': 'bbb',
                                                                               'disk_type': 'ccc'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_gcp_network_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'GCP', 'cluster', 'create', '--label', 'test_label',
                    '--vpc-id', 'vpc-1', '--subnet-id', 'subnet-1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config': {'network_config':
                                                                                          {'subnet': 'subnet-1',
                                                                                           'network': 'vpc-1'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_gcp_location_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'GCP', 'cluster', 'create', '--label', 'test_label',
                    '--gcp-region', 'xxx', '--gcp-zone', 'yyy']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config': {'location':
                                                                                          {'region': 'xxx',
                                                                                           'zone': 'yyy'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_gcp_cluster_composition(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'GCP', 'cluster', 'create', '--label', 'test_label',
                    '--master-preemptible',
                    '--min-nodes-preemptible', '--min-nodes-preemptible-percentage', '50',
                    '--autoscaling-nodes-preemptible', '--autoscaling-nodes-preemptible-percentage', '75']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {
                                                    'cloud_config': {
                                                        'cluster_composition': {
                                                            'master': {
                                                                'preemptible': True
                                                            },
                                                            'min_nodes': {
                                                                'preemptible': True,
                                                                'percentage': 50
                                                            },
                                                            'autoscaling_nodes': {
                                                                'preemptible': True,
                                                                'percentage': 75
                                                            }
                                                        }
                                                    },
                                                    'cluster_info': {
                                                        'label': ['test_label']
                                                    }
                                                })

    def test_gcp_cluster_composition_invalid(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'GCP', 'cluster', 'create', '--label', 'test_label',
                    '--master-preemptible',
                    '--min-nodes-preemptible', '--min-nodes-preemptible-percentage', 'invalid_value']
        Qubole.cloud = None
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_presto_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                        '--flavour', 'presto', '--enable-rubix', '--presto-custom-config', temp.name]
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                                                    {'engine_config':
                                                         {'flavour': 'presto',
                                                          'presto_settings': {
                                                              'custom_presto_config': 'config.properties:\na=1\nb=2'},
                                                          'hadoop_settings': {
                                                              'enable_rubix': True
                                                          }},
                                                     'cluster_info': {'label': ['test_label']}})

    def test_hs2_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label',
                        'test_label', '--flavour', 'hs2', '--node-bootstrap-file', 'test_file_name',
                        '--slave-instance-type', 'c1.xlarge', '--min-nodes', '3', '--parent-cluster-id', '1']
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                                                    {'engine_config':
                                                         {'flavour': 'hs2'},
                                                     'cluster_info': {'label': ['test_label'],
                                                                      'parent_cluster_id': 1,
                                                                      'min_nodes': 3,
                                                                      'node_bootstrap': 'test_file_name',
                                                                      'slave_instance_type': 'c1.xlarge'}})

    def test_hs2_parent_cluster_label(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label',
                        'test_label', '--flavour', 'hs2', '--node-bootstrap-file', 'test_file_name',
                        '--slave-instance-type', 'c1.xlarge', '--min-nodes', '3', '--parent-cluster-label',
                        'parent_cluster_label']
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                                                    {'engine_config':
                                                         {'flavour': 'hs2'},
                                                     'cluster_info': {'label': ['test_label'],
                                                                      'parent_cluster_label': 'parent_cluster_label',
                                                                      'min_nodes': 3,
                                                                      'node_bootstrap': 'test_file_name',
                                                                      'slave_instance_type': 'c1.xlarge'}})

    def test_spark_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                        '--flavour', 'spark', '--custom-spark-config', 'spark-overrides']
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                                                    {'engine_config':
                                                         {'flavour': 'spark',
                                                          'spark_settings': {
                                                              'custom_spark_config': 'spark-overrides'}},
                                                     'cluster_info': {'label': ['test_label'], }})

    def test_sparkstreaming_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                        '--flavour', 'sparkstreaming', '--custom-spark-config', 'spark-overrides']
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                                                    {'engine_config':
                                                         {'flavour': 'sparkstreaming',
                                                          'spark_settings': {
                                                              'custom_spark_config': 'spark-overrides'}},
                                                     'cluster_info': {'label': ['test_label'], }})

    def test_airflow_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                        '--flavour', 'airflow', '--dbtap-id', '1', '--fernet-key', '-1', '--overrides',
                        'airflow_overrides', '--airflow-version', '1.10.0', '--airflow-python-version', '2.7']
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                                                    {'engine_config':
                                                         {'flavour': 'airflow',
                                                          'airflow_settings': {
                                                              'dbtap_id': '1',
                                                              'fernet_key': '-1',
                                                              'overrides': 'airflow_overrides',
                                                              'version': '1.10.0',
                                                              'airflow_python_version': '2.7'
                                                          }},
                                                     'cluster_info': {'label': ['test_label'], }})

    def test_mlflow_engine_config(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--flavour', 'mlflow', '--mlflow-version', '1.7', '--mlflow-dbtap-id', '-1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'engine_config':
                                                     {'flavour': 'mlflow',
                                                      'mlflow_settings': {
                                                          'version': '1.7',
                                                          'dbtap_id': '-1'
                                                      }},
                                                 'cluster_info': {'label': ['test_label'], }})

    def test_hive_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                        '--flavour', 'hadoop2', '--hive_version', '2.3']
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                                                    {'engine_config':
                                                         {'flavour': 'hadoop2',
                                                          'hive_settings': {
                                                              'hive_version': '2.3'
                                                          }},
                                                     'cluster_info': {'label': ['test_label'],}})



    def test_persistent_security_groups_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--persistent-security-groups', 'sg1, sg2']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info':
                                                     {'label': ['test_label']},
                                                 'cloud_config':
                                                     {'network_config':
                                                          {'persistent_security_groups': 'sg1, sg2'}
                                                      }
                                                 })

    def test_data_disk_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--count', '1', '--size', '100', '--disk-type', 'standard']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info': {
                                                    'datadisk': {'count': 1, 'type': 'standard', 'size': 100},
                                                    'label': ['test_label']}})

    def test_heterogeneous_config_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--heterogeneous-config', 'test']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info':
                                                     {'label': ['test_label'],
                                                      'heterogeneous_config': 'test'
                                                      }
                                                 })

    def test_image_override(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--image-overrides', 'test/image1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info': {'label': ['test_label']},
                                                 'internal': {'image_uri_overrides': 'test/image1'}
                                                 })

    def test_image_version_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label',
                    'test_label', '--flavour', 'hadoop2', '--slave-instance-type', 'c1.xlarge', '--min-nodes', '3',
                    '--image-version', '1.latest']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'engine_config':
                                                     {'flavour': 'hadoop2'},
                                                 'cluster_info': {'label': ['test_label'],
                                                                  'min_nodes': 3,
                                                                  'slave_instance_type': 'c1.xlarge',
                                                                  'cluster_image_version': '1.latest'}})

    def test_spot_block_duration_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--spot-block-duration', '120']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info':
                                                     {'spot_settings':
                                                          {'spot_block_settings': {'duration': 120}},
                                                      'label': ['test_label']}})

    def test_slave_request_type_spotblock_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--slave-request-type', 'spotblock']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info': {'label': ['test_label'],
                                                                  'slave_request_type': 'spotblock'}})

    def test_node_base_cooldown_period_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--node-base-cooldown-period', '10']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info': {'label': ['test_label'],
                                                                  'node_base_cooldown_period': 10}})

    def test_node_base_cooldown_period_invalid_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--node-base-cooldown-period', 'invalid_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_node_spot_cooldown_period_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--node-spot-cooldown-period', '15']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info': {'label': ['test_label'],
                                                                  'node_volatile_cooldown_period': 15}})

    def test_node_spot_cooldown_period_invalid_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--node-spot-cooldown-period', 'invalid_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_env_settings_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--env-name', 'test_env', '--python-version', '2.7', '--r-version', '3.3']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info': {'label': ['test_label'],
                                                                  'env_settings': {'name': 'test_env',
                                                                                   'python_version': '2.7',
                                                                                   'r_version': '3.3'}}})

    def test_root_disk_size_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--root-disk-size', '100']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info': {'label': ['test_label'],
                                                                  'rootdisk': {'size': 100}}})

    def test_root_disk_size_invalid_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--root-disk-size', 'invalid_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_disable_start_stop(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--disable-cluster-pause', '--disable-autoscale-node-pause']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {
                                                    'cluster_info': {
                                                        'label': ['test_label'],
                                                        'disable_cluster_pause': 1,
                                                        'disable_autoscale_node_pause': 1
                                                    }
                                                })

    def test_start_stop_timeouts(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--no-disable-cluster-pause', '--paused-cluster-timeout', '30',
                    '--no-disable-autoscale-node-pause', '--paused-autoscale-node-timeout', '60']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {
                                                    'cluster_info': {
                                                        'label': ['test_label'],
                                                        'disable_cluster_pause': 0,
                                                        'paused_cluster_timeout_mins': 30,
                                                        'disable_autoscale_node_pause': 0,
                                                        'paused_autoscale_node_timeout_mins': 60
                                                    }
                                                })

    def test_start_stop_timeouts_invalid(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                    '--paused-cluster-timeout', 'invalid_value', '--paused-autoscale-node-timeout', 'invalid_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_notifications_given(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                        '--notification-channels', '7']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters',
                                                {'cluster_info': {'label': ['test_label']},
                                                 'monitoring': {'notifications': {'all': [7]}}})

class TestClusterUpdate(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123', {})

    def test_aws_cloud_config(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--compute-access-key', 'aki', '--aws-region', 'us-east-1',
                    '--bastion-node-public-dns', 'dummydns']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123', {'cloud_config':
                                                                            {'compute_config':
                                                                                 {'compute_access_key': 'aki'},
                                                                             'location':
                                                                                 {'aws_region': 'us-east-1'},
                                                                             'network_config':
                                                                                 {
                                                                                     'bastion_node_public_dns': 'dummydns'}}
                                                                        })

    def test_azure_cloud_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'update', '123',
                    '--vnet-name', 'testvnet', '--storage-account-name', 'test_account_name',
                    '--compute-subscription-id', 'testsubscriptionid']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123', {'cloud_config': {'compute_config':
                                                                                             {
                                                                                                 'compute_subscription_id': 'testsubscriptionid'},
                                                                                         'storage_config': {
                                                                                             'storage_account_name': 'test_account_name'},
                                                                                         'network_config': {
                                                                                             'vnet_name': 'testvnet'}}})

    def test_azure_master_static_nic(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'update', '123',
                    '--vnet-name', 'testvnet', '--subnet-name', 'testsubnet',
                    '--vnet-resource-group-name', 'vnetresname', '--master-static-nic-name', 'nic1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cloud_config': {
                                                    'network_config': {'vnet_resource_group_name': 'vnetresname',
                                                                       'subnet_name': 'testsubnet',
                                                                       'vnet_name': 'testvnet',
                                                                       'master_static_nic_name': 'nic1'}}})

    def test_azure_master_static_pip(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'AZURE', 'cluster', 'update', '123',
                    '--vnet-name', 'testvnet', '--subnet-name', 'testsubnet',
                    '--vnet-resource-group-name', 'vnetresname', '--master-static-public-ip-name', 'pip1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cloud_config': {
                                                    'network_config': {'vnet_resource_group_name': 'vnetresname',
                                                                       'subnet_name': 'testsubnet',
                                                                       'vnet_name': 'testvnet',
                                                                       'master_static_public_ip_name': 'pip1'}}})

    def test_oracle_bmc_cloud_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'update', '123',
                    '--oracle-region', 'us-phoenix-1', '--compartment-id', 'abc-compartment',
                    '--storage-tenant-id', 'xxx11', '--compute-user-id', 'yyyy11']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123', {'cloud_config':
                                                                            {'network_config':
                                                                                 {'compartment_id': 'abc-compartment'},
                                                                             'compute_config': {
                                                                                 'compute_user_id': 'yyyy11'},
                                                                             'storage_config': {
                                                                                 'storage_tenant_id': 'xxx11'},
                                                                             'location': {'region': 'us-phoenix-1'}
                                                                             }
                                                                        })

    def test_oracle_opc_cloud_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_OPC', 'cluster', 'update', '123',
                    '--acl', 'acl_1', '--rest-api-endpoint', 'rest_api_endpoint_1',
                    '--storage-rest-api-endpoint', 'storage_rest_api_endpoint_1']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123', {'cloud_config':
                                                                            {'network_config':
                                                                                 {'acl': 'acl_1'},
                                                                             'compute_config': {
                                                                                 'rest_api_endpoint': 'rest_api_endpoint_1'},
                                                                             'storage_config': {
                                                                                 'storage_rest_api_endpoint': 'storage_rest_api_endpoint_1'}
                                                                             }
                                                                        })

    def test_gcp_cloud_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'GCP', 'cluster', 'update', '123',
                    '--gcp-region', 'xxx', '--subnet-id', 'abc-subnet',
                    '--storage-client-email', 'xxx11', '--qsa-client-id', 'yyyy11']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123', {'cloud_config':
                                                                            {'network_config':
                                                                                 {'subnet': 'abc-subnet'},
                                                                             'compute_config': {
                                                                                 'qsa_client_id': 'yyyy11'},
                                                                             'storage_config': {
                                                                                 'inst_client_email': 'xxx11'},
                                                                             'location': {'region': 'xxx'}
                                                                             }
                                                                        })

    def test_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("a=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                        '--use-qubole-placement-policy', '--enable-rubix',
                        '--custom-hadoop-config', temp.name]
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('PUT', 'clusters/123', {'engine_config':
                                                                                {'hadoop_settings':
                                                                                     {
                                                                                         'use_qubole_placement_policy': True,
                                                                                         'custom_hadoop_config': 'a=1\nb=2',
                                                                                         'enable_rubix': True}}
                                                                            })

    def test_cluster_info(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--slave-request-type', 'ondemand', '--max-nodes', '6',
                    '--disable-ganglia-monitoring']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123', {'monitoring': {'ganglia': False},
                                                                        'cluster_info': {
                                                                            'slave_request_type': 'ondemand',
                                                                            'max_nodes': 6}})

    def test_spot_block_duration_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123', '--spot-block-duration', '120']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cluster_info':
                                                     {'spot_settings':
                                                          {'spot_block_settings': {'duration': 120}}}})

    def test_slave_request_type_spotblock_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--slave-request-type', 'spotblock']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cluster_info': {'slave_request_type': 'spotblock'}})

    def test_node_base_cooldown_period_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--node-base-cooldown-period', '10']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cluster_info': {'node_base_cooldown_period': 10}})

    def test_node_base_cooldown_period_invalid_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--node-base-cooldown-period', 'invalid_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_node_spot_cooldown_period_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--node-spot-cooldown-period', '15']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cluster_info': {'node_volatile_cooldown_period': 15}})

    def test_node_spot_cooldown_period_invalid_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--node-spot-cooldown-period', 'invalid_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_root_disk_size_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--root-disk-size', '100']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',
                                                {'cluster_info': {
                                                    'rootdisk': {'size': 100}}})

    def test_root_disk_size_invalid_v2(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                    '--root-disk-size', 'invalid_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestClusterClone(QdsCliTestCase):
    def test_minimal(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'clone', '1234', '--label', 'test_label1', 'test_label2']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/clone', {'cluster_info':
                                                                                    {'label': ['test_label1',
                                                                                               'test_label2']}})


class TestClusterList(QdsCliTestCase):
    def test_id(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--id', '123']
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters/123", params=None)

    def test_label(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--label', 'test_label']
        print_command()
        Connection._api_call = Mock(return_value={"provider": "aws"})
        qds.main()
        Connection._api_call.assert_called_with("GET", "clusters/test_label", params=None)

    def test_minimal(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_up(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'up']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster": {"state": "up"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_down(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'down']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster": {"state": "down"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_terminating(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'terminating']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster": {"state": "terminating"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_pending(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'pending']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster": {"state": "pending"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_invalid(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'invalid']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster": {"state": "invalid"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_page(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--page', '2']
        Qubole.cloud = None
        params = {"page": 2}
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster": {"state": "up"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=params)

    def test_page_invalid(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--page', 'string_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()

    def test_per_page(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--per-page', '5']
        Qubole.cloud = None
        params = {"per_page": 5}
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster": {"state": "up"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=params)

    def test_per_page_invalid(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--per-page', 'string_value']
        print_command()
        with self.assertRaises(SystemExit):
            qds.main()


class TestClusterShow(QdsCliTestCase):
    def test_connection(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--label', 'test_label']
        print_command()
        Connection.__init__ = Mock(return_value=None)
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection.__init__.assert_called_with(ANY, 'https://qds.api.url/api/v2', ANY, ANY, ANY, ANY)


class TestClusterStatus(QdsCliTestCase):
    def test_status_api(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'status', '123']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"state": "DOWN"}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters/123/state', params=None)
