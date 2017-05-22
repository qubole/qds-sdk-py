from qds_sdk.cloud.cloud import Cloud
from qds_sdk.commands import Command,PrestoCommand

import re
import logging
import sys

log = logging.getLogger("qds_commands")

Azure_URI_RE = re.compile(r'wasb://([^@]+)([^/]+)/?(.*)')

class AzureCloud(Cloud):
    '''
    qds_sdk.cloud.AzureCloud is the class which stores information about azure cloud config settings.
    The objects of this class can be use to set azure cloud_config settings while create/update/clone a cluster.
    '''

    def __init__(self):
        self.compute_config = {}
        self.location = {}
        self.network_config = {}
        self.storage_config = {}

    def set_cloud_config(self,
                         compute_client_id=None,
                         compute_client_secret=None,
                         compute_subscription_id=None,
                         compute_tenant_id=None,
                         use_account_compute_creds=None,
                         location=None,
                         storage_access_key=None,
                         storage_account_name=None,
                         disk_storage_account_name=None,
                         disk_storage_account_resource_group_name=None,
                         persistent_security_groups=None,
                         bastion_node_public_dns=None,
                         vnet_name=None,
                         subnet_name=None,
                         vnet_resource_group_name=None,
                         master_elastic_ip=None):
        '''

        Args:
            compute_client_id: Client id for azure cluster

            compute_client_secret: Client secret key for azure cluster

            compute_subscription_id: Subscription id for azure cluster

            compute_tenant_id: Tenant id for azure cluster

            use_account_compute_creds: Set it to true to use the account's compute
                credentials for all clusters of the account.The default value is false

            location: Location for azure cluster

            storage_access_key: Storage access key for azure cluster

            storage_account_name: Storage account name for azure cluster

            disk_storage_account_name: Disk storage account name for azure cluster

            disk_storage_account_resource_group_name: Disk storage account resource group
                namefor azure cluster

            persistent_security_groups: security group to associate with each node of the cluster.
                Typically used to provide access to external hosts

            bastion_node_public_dns: public dns name of the bastion node.
                Required only if cluster is in private subnet of a EC2-VPC

            vnet_name: vnet name for azure

            subnet_name: subnet name for azure

            vnet_resource_group_name: vnet resource group name for azure

            master_elastic_ip: It is the Elastic IP address for attaching to the cluster master

        '''

        self.set_compute_config(use_account_compute_creds, compute_tenant_id,
                                compute_subscription_id, compute_client_id,
                                compute_client_secret)
        self.set_location(location)
        self.set_network_config(bastion_node_public_dns, persistent_security_groups,
                                master_elastic_ip, vnet_name, subnet_name,
                                vnet_resource_group_name)
        self.set_storage_config(storage_access_key, storage_account_name,
                                disk_storage_account_name,
                                disk_storage_account_resource_group_name)

    def set_compute_config(self,
                           use_account_compute_creds=None,
                           compute_tenant_id=None,
                           compute_subscription_id=None,
                           compute_client_id=None,
                           compute_client_secret=None):
        self.compute_config['use_account_compute_creds'] = use_account_compute_creds
        self.compute_config['compute_tenant_id'] = compute_tenant_id
        self.compute_config['compute_subscription_id'] = compute_subscription_id
        self.compute_config['compute_client_id'] = compute_client_id
        self.compute_config['compute_client_secret'] = compute_client_secret

    def set_location(self,
                     location=None):
        self.location['location'] = location

    def set_network_config(self,
                           bastion_node_public_dns=None,
                           persistent_security_groups=None,
                           master_elastic_ip=None,
                           vnet_name=None,
                           subnet_name=None,
                           vnet_resource_group_name=None):
        self.network_config['bastion_node_public_dns'] = bastion_node_public_dns
        self.network_config['persistent_security_groups'] = persistent_security_groups
        self.network_config['master_elastic_ip'] = master_elastic_ip
        self.network_config['vnet_name'] = vnet_name
        self.network_config['subnet_name'] = subnet_name
        self.network_config['vnet_resource_group_name'] = vnet_resource_group_name

    def set_storage_config(self,
                           storage_access_key=None,
                           storage_account_name=None,
                           disk_storage_account_name=None,
                           disk_storage_account_resource_group_name=None):
        self.storage_config['storage_access_key'] = storage_access_key
        self.storage_config['storage_account_name'] = storage_account_name
        self.storage_config['disk_storage_account_name'] = disk_storage_account_name
        self.storage_config['disk_storage_account_resource_group_name'] \
            = disk_storage_account_resource_group_name

    def set_cloud_config_from_arguments(self, arguments):
        self.set_cloud_config(compute_client_id=arguments.compute_client_id,
                              compute_client_secret=arguments.compute_client_secret,
                              compute_subscription_id=arguments.compute_subscription_id,
                              compute_tenant_id=arguments.compute_tenant_id,
                              use_account_compute_creds=arguments.use_account_compute_creds,
                              location=arguments.location,
                              storage_access_key=arguments.storage_access_key,
                              storage_account_name=arguments.storage_account_name,
                              disk_storage_account_name=arguments.disk_storage_account_name,
                              disk_storage_account_resource_group_name=arguments.disk_storage_account_resource_group_name,
                              vnet_name=arguments.vnet_name,
                              subnet_name=arguments.subnet_name,
                              vnet_resource_group_name=arguments.vnet_resource_group_name)

    def create_parser(self, argparser):
        # compute settings parser
        compute_config = argparser.add_argument_group("compute config settings")
        compute_creds = compute_config.add_mutually_exclusive_group()
        compute_creds.add_argument("--enable-account-compute-creds",
                                   dest="use_account_compute_creds",
                                   action="store_true",
                                   default=None,
                                   help="to use account compute credentials")
        compute_creds.add_argument("--disable-account-compute-creds",
                                   dest="use_account_compute_creds",
                                   action="store_false",
                                   default=None,
                                   help="to disable account compute credentials")
        compute_config.add_argument("--compute-client-id",
                                    dest="compute_client_id",
                                    default=None,
                                    help="client id for azure cluster")
        compute_config.add_argument("--compute-client-secret",
                                    dest="compute_client_secret",
                                    default=None,
                                    help="client secret key for azure cluster")
        compute_config.add_argument("--compute-tenant-id",
                                    dest="compute_tenant_id",
                                    default=None,
                                    help="tenant id for azure cluster")
        compute_config.add_argument("--compute-subscription-id",
                                    dest="compute_subscription_id",
                                    default=None,
                                    help="Subscription id for azure cluster")

        # location settings parser
        location_group = argparser.add_argument_group("location config settings")
        location_group.add_argument("--location",
                                    dest="location",
                                    default=None,
                                    help="location for azure cluster")

        # network settings parser
        network_config_group = argparser.add_argument_group("network config settings")
        network_config_group.add_argument("--vnet-name",
                                          dest="vnet_name",
                                          help="vnet name for azure", )
        network_config_group.add_argument("--subnet-name",
                                          dest="subnet_name",
                                          help="subnet name for azure")
        network_config_group.add_argument("--vnet-resource-group-name",
                                          dest="vnet_resource_group_name",
                                          help="vnet resource group name for azure")

        # storage config settings parser
        storage_config = argparser.add_argument_group("storage config settings")
        storage_config.add_argument("--storage-access-key",
                                    dest="storage_access_key",
                                    default=None,
                                    help="storage access key for azure cluster")
        storage_config.add_argument("--storage-account-name",
                                    dest="storage_account_name",
                                    default=None,
                                    help="storage account name for azure cluster")
        storage_config.add_argument("--disk-storage-account-name",
                                    dest="disk_storage_account_name",
                                    default=None,
                                    help="disk storage account name for azure cluster")
        storage_config.add_argument("--disk-storage-account-resource-group-name",
                                    dest="disk_storage_account_resource_group_name",
                                    default=None,
                                    help="disk storage account resource group for azure cluster")


    def get_results(self, fp=sys.stdout, storage_credentials=None, r={}, delim=None, qlog=None, include_header=None):
        from azure.storage.blob import BlockBlobService
        account_name = storage_credentials.get('storage_config').get('account_name')
        account_key = storage_credentials.get('storage_config').get('access_key')
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
        log.info("Starting download from result locations: [%s]" % ",".join(r['result_location']))
        num_result_dir = Command.find(self.id).num_result_dir
        for azure_path in r['result_location']:
            _download_to_local_azure(block_blob_service, azure_path, fp, delim=delim,
                                     skip_data_avail_check=isinstance(self, PrestoCommand))


def _download_to_local_azure(blob_service, azure_path, fp, delim=None, skip_data_avail_check=False):
    def get_contents_to_file_using_delim():
        if sys.version_info < (3, 0, 0):
            fp.write(str(content).replace(chr(1), delim))
        else:
            import io
            if isinstance(fp, io.TextIOBase):
                fp.buffer.write(content.decode('utf-8').replace(chr(1), delim).encode('utf8'))
            elif isinstance(fp, io.BufferedIOBase) or isinstance(fp, io.RawIOBase):
                fp.write(content.decode('utf8').replace(chr(1), delim).encode('utf8'))
            else:
                # Can this happen? Don't know what's the right thing to do in this case.
                pass

    def get_contents_to_file_azure():
        if sys.version_info < (3, 0, 0):
            fp.write(content.encode('utf8'))
        else:
            import io
            if isinstance(fp, io.TextIOBase):
                fp.buffer.write(content.encode('utf8'))
            elif isinstance(fp, io.BufferedIOBase) or isinstance(fp, io.RawIOBase):
                fp.write(content.encode('utf8'))
            else:
                # Can this happen? Don't know what's the right thing to do in this case.
                pass


    try:
        m = Azure_URI_RE.match(azure_path)
        container_name = m.group(1)
        blob_name = m.group(3)
        if azure_path.endswith('/') is False:

            response = blob_service.get_blob_to_bytes(container_name, blob_name)
            content = response.content

            if delim is not None:
                get_contents_to_file_using_delim()
            else:
                get_contents_to_file_azure()
        else:
            # Write check for results if incomplete data available
            # For folders
            list = blob_service.list_blobs(container_name, prefix=blob_name)
            for blob in list:
                name = blob.name
                if name.endswith('/') is False:
                    log.info("Downloading file from %s" % name)
                    response = blob_service.get_blob_to_bytes(container_name, name)
                    content = response.content
                    if delim is not None:
                        get_contents_to_file_using_delim()
                    else:
                        get_contents_to_file_azure()
    except Exception as e:
        raise Exception("Not able to download results: %s" % e.message)
