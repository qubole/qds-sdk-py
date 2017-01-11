#import qds_sdk.cloud.cloud
class AzureCloud:

    def __init__(self):
        self.compute_config = {}
        self.location = {}
        self.network_config = {}
        self.storage_config = {}

    def cloud_config_parser(self, argparser):

        # compute settings
        compute_config = argparser.add_argument_group("compute config settings")
        compute_creds = compute_config.add_mutually_exclusive_group()
        compute_creds.add_argument("--enable_account_compute_creds",
                                   dest="use_account_compute_creds",
                                   action="store_true",
                                   default=None,
                                   help="to use account compute credentials")
        compute_creds.add_argument("--disable_account_compute_creds",
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
                                    help="client id for azure cluster")
        compute_config.add_argument("--compute-tenant-id",
                                    dest="compute_tenant_id",
                                    default=None,
                                    help="tenant id for azure cluster")
        compute_config.add_argument("--compute-subscription-id",
                                    dest="compute_subscription_id",
                                    default=None,
                                    help="Subscription id for azure cluster")


        # location settings
        location_group = argparser.add_argument_group("location config settings")
        location_group.add_argument("--location",
                                    dest="location",
                                    default=None,
                                    help="location for azure cluster")

        # network settings
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
        network_config_group.add_argument("--bastion-node-public-dns",
                                          dest="bastion_node_public_dns",
                                          help="public dns name of the bastion node. "
                                               "Required only if cluster is in private subnet of a EC2-VPC", )
        network_config_group.add_argument("--persistent-security-groups",
                                          dest="persistent_security_groups",
                                          help="a security group to associate with each" +
                                               " node of the cluster. Typically used" +
                                               " to provide access to external hosts", )
        network_config_group.add_argument("--master-elastic-ip",
                                          dest="master_elastic_ip",
                                          help="master elastic ip for cluster")

        # storage config settings
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
















