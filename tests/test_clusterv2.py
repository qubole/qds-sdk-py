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
                        '--bastion-node-public-dns', 'dummydns','--persistent-security-groups',
                        'foopsg','--master-elastic-ip', "10.10.10.10"]
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config': {'compute_config': {'use_account_compute_creds': True},
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
                                                                                          {'compute_key_finger_print': 'zzz22',
                                                                                           'compute_api_private_rsa_key': 'aaa',
                                                                                           'compute_user_id': 'yyyy11',
                                                                                           'compute_tenant_id': 'xxx11'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_oracle_bmc_storage_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'create', '--label', 'test_label',
                    '--storage-tenant-id', 'xxx11', '--storage-user-id', 'yyyy11', '--storage-key-finger-print',
                    'zzz22', '--storage-api-private-rsa-key', 'aaa']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters', {'cloud_config':
                                                                         {'storage_config':
                                                                              {'storage_key_finger_print': 'zzz22',
                                                                               'storage_api_private_rsa_key': 'aaa',
                                                                               'storage_user_id': 'yyyy11',
                                                                               'storage_tenant_id': 'xxx11'}},
                                                                     'cluster_info': {'label': ['test_label']}})

    def test_oracle_bmc_network_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'create', '--label', 'test_label',
                    '--compartment-id', 'abc-compartment', '--image-id', 'abc-image', '--vcn-id', 'vcn-1',
                    '--subnet-id', 'subnet-1' ]
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



    def test_presto_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("config.properties:\na=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'create', '--label', 'test_label',
                        '--flavour', 'presto', '--presto-custom-config', temp.name]
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('POST', 'clusters',
                                                    {'engine_config':
                                                         {'flavour': 'presto',
                                                          'presto_settings': {
                                                              'custom_presto_config': 'config.properties:\na=1\nb=2'}},
                                                     'cluster_info': {'label': ['test_label']}})

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
                                                {'cluster_info':{'label': ['test_label']},
                                                'internal':{'image_uri_overrides': 'test/image1'}
                                                })


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
                                                                                 {'bastion_node_public_dns': 'dummydns'}}
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
                                                                                             {'compute_subscription_id': 'testsubscriptionid'},
                                                                                         'storage_config': {'storage_account_name': 'test_account_name'},
                                                                                         'network_config': {'vnet_name': 'testvnet'}}})

    def test_oracle_bmc_cloud_config(self):
        sys.argv = ['qds.py', '--version', 'v2', '--cloud', 'ORACLE_BMC', 'cluster', 'update', '123',
                    '--oracle-region', 'us-phoenix-1', '--compartment-id', 'abc-compartment',
                    '--storage-tenant-id', 'xxx11', '--compute-user-id', 'yyyy11']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('PUT', 'clusters/123',  {'cloud_config':
                                                                             {'network_config':
                                                                                  {'compartment_id': 'abc-compartment'},
                                                                              'compute_config': {'compute_user_id': 'yyyy11'},
                                                                              'storage_config': {'storage_tenant_id': 'xxx11'},
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
        Connection._api_call.assert_called_with('PUT', 'clusters/123',  {'cloud_config':
                                                                             {'network_config':
                                                                                  {'acl': 'acl_1'},
                                                                              'compute_config': {'rest_api_endpoint': 'rest_api_endpoint_1'},
                                                                              'storage_config': {'storage_rest_api_endpoint': 'storage_rest_api_endpoint_1'}
                                                                              }
                                                                         })


    def test_engine_config(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("a=1\nb=2".encode("utf8"))
            temp.flush()
            sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'update', '123',
                        '--use-qubole-placement-policy', '--custom-hadoop-config',
                        temp.name]
            Qubole.cloud = None
            print_command()
            Connection._api_call = Mock(return_value={})
            qds.main()
            Connection._api_call.assert_called_with('PUT', 'clusters/123',  {'engine_config':
                                                                                 {'hadoop_settings':
                                                                                      {'use_qubole_placement_policy': True,
                                                                                       'custom_hadoop_config': 'a=1\nb=2'}}
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


class TestClusterClone(QdsCliTestCase):

    def test_minimal(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'clone', '1234', '--label', 'test_label1', 'test_label2']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value={})
        qds.main()
        Connection._api_call.assert_called_with('POST', 'clusters/1234/clone', {'cluster_info':
                                                                                    {'label': ['test_label1', 'test_label2']}})

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
        Connection._api_call = Mock(return_value=[{"cluster" : {"state" : "up"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_down(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'down']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster" : {"state" : "down"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_terminating(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'terminating']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster" : {"state" : "terminating"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_pending(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'pending']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster" : {"state" : "pending"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

    def test_state_invalid(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'list', '--state', 'invalid']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"cluster" : {"state" : "invalid"}}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters', params=None)

class TestClusterStatus(QdsCliTestCase):

    def test_status_api(self):
        sys.argv = ['qds.py', '--version', 'v2', 'cluster', 'status', '123']
        Qubole.cloud = None
        print_command()
        Connection._api_call = Mock(return_value=[{"state": "DOWN"}])
        qds.main()
        Connection._api_call.assert_called_with('GET', 'clusters/123/state', params=None)
