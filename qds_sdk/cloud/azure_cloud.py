from qds_sdk.cloud.cloud import Cloud
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
                         managed_disk_account_type=None,
                         persistent_security_groups=None,
                         bastion_node_public_dns=None,
                         vnet_name=None,
                         subnet_name=None,
                         vnet_resource_group_name=None,
                         master_elastic_ip=None,
                         master_static_nic_name=None,
                         master_static_public_ip_name=None,
                         resource_group_name=None):
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

            managed_disk_account_type:  Managed Disk Type for azure cluster

            persistent_security_groups: security group to associate with each node of the cluster.
                Typically used to provide access to external hosts

            bastion_node_public_dns: public dns name of the bastion node.
                Required only if cluster is in private subnet of a EC2-VPC

            vnet_name: vnet name for azure

            subnet_name: subnet name for azure

            vnet_resource_group_name: vnet resource group name for azure

            master_elastic_ip: It is the Elastic IP address for attaching to the cluster master

            master_static_nic_name: Name of NIC that has to be attached to cluster's master node

            master_static_public_ip_name: Name of Static Public Ip address that has to be attached
                to cluster's master node

            resource_group_name: Resource group for cluster
        '''

        self.set_compute_config(use_account_compute_creds, compute_tenant_id,
                                compute_subscription_id, compute_client_id,
                                compute_client_secret)
        self.set_location(location)
        self.set_network_config(bastion_node_public_dns, persistent_security_groups,
                                master_elastic_ip, vnet_name, subnet_name,
                                vnet_resource_group_name, master_static_nic_name, master_static_public_ip_name)
        self.set_storage_config(storage_access_key, storage_account_name,
                                disk_storage_account_name,
                                disk_storage_account_resource_group_name,
                                managed_disk_account_type)
        self.resource_group_name = resource_group_name

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
                           vnet_resource_group_name=None,
                           master_static_nic_name=None,
                           master_static_public_ip_name=None):
        self.network_config['bastion_node_public_dns'] = bastion_node_public_dns
        self.network_config['persistent_security_groups'] = persistent_security_groups
        self.network_config['master_elastic_ip'] = master_elastic_ip
        self.network_config['vnet_name'] = vnet_name
        self.network_config['subnet_name'] = subnet_name
        self.network_config['vnet_resource_group_name'] = vnet_resource_group_name
        self.network_config['master_static_nic_name'] = master_static_nic_name
        self.network_config['master_static_public_ip_name'] = master_static_public_ip_name

    def set_storage_config(self,
                           storage_access_key=None,
                           storage_account_name=None,
                           disk_storage_account_name=None,
                           disk_storage_account_resource_group_name=None,
                           managed_disk_account_type=None):
        self.storage_config['storage_access_key'] = storage_access_key
        self.storage_config['storage_account_name'] = storage_account_name
        self.storage_config['disk_storage_account_name'] = disk_storage_account_name
        self.storage_config['disk_storage_account_resource_group_name'] \
            = disk_storage_account_resource_group_name
        self.storage_config['managed_disk_account_type'] = managed_disk_account_type

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
                              managed_disk_account_type=arguments.managed_disk_account_type,
                              vnet_name=arguments.vnet_name,
                              subnet_name=arguments.subnet_name,
                              vnet_resource_group_name=arguments.vnet_resource_group_name,
                              master_static_nic_name=arguments.master_static_nic_name,
                              master_static_public_ip_name=arguments.master_static_public_ip_name,
                              resource_group_name=arguments.resource_group_name)

    def set_composition_arguments(self, comp_group):
        # composition arguments we want to accept for azure
        comp_group.add_argument("--min-ondemand-percentage",
                                dest="min_ondemand_percentage",
                                type=int, default=0,
                                help="Percentage of ondemand nodes in min config.")
        comp_group.add_argument("--min-spot-percentage",
                                dest="min_spot_percentage",
                                type=int, default=0,
                                help="Percentage of spot nodes in min config.")
        comp_group.add_argument("--max-price-percentage",
                                dest="max_price_percentage",
                                type=int, default=100,
                                help="Percentage of maximum price percentage"
                                     " for spot nodes.")
        comp_group.add_argument("--min-spot-fallback",
                                dest="min_spot_fallback",
                                choices=["ondemand", None],
                                default=None,
                                help="Whether to fallback to on-demand instances" +
                                     " for min nodes if spot instances" +
                                     " aren't available.")
        comp_group.add_argument("--autoscaling-ondemand-percentage",
                                dest="autoscaling_ondemand_percentage",
                                type=int, default=0,
                                help="Percentage of ondemand nodes" +
                                     "in autoscaling config.")
        comp_group.add_argument("--autoscaling-spot-percentage",
                                dest="autoscaling_spot_percentage",
                                type=int, default=0,
                                help="Percentage of spot nodes" +
                                "in autoscaling config.")
        comp_group.add_argument("--autoscaling-spot-fallback",
                                dest="autoscaling_spot_fallback",
                                choices=["ondemand", None],
                                default=None,
                                help="Whether to fallback to on-demand instances" +
                                     " for autoscaling nodes if spot instances" +
                                     " aren't available.")

    # Ignore other key-value arguments.
    def get_composition(self,
                        min_ondemand_percentage=0,
                        min_spot_percentage=0,
                        min_spot_fallback=None,
                        autoscaling_ondemand_percentage=0,
                        autoscaling_spot_percentage=0,
                        autoscaling_spot_fallback=None,
                        max_price_percentage=100,
                        **kwargs):
        composition = {}
        composition["min_nodes"] = {"nodes": []}
        min_nodes = composition["min_nodes"]["nodes"]
        if min_ondemand_percentage + min_spot_percentage != 100:
            raise ValueError("Minimum nodes ondemand+spot percentage"
                             " should be 100: Ondemand pct: %d Spot pct: %d"
                             % (min_ondemand_percentage,
                                min_spot_percentage))
        if min_ondemand_percentage > 0:
            min_nodes.append({"type": "ondemand",
                              "percentage": min_ondemand_percentage})
        if min_spot_percentage > 0:
            min_nodes.append({"type": "spot", "percentage": min_spot_percentage,
                              "fallback": min_spot_fallback,
                              "max_price_percentage": max_price_percentage})

        composition["autoscaling_nodes"] = {"nodes": []}
        autoscaling_nodes = composition["autoscaling_nodes"]["nodes"]
        if autoscaling_ondemand_percentage + autoscaling_spot_percentage != 100:
            raise ValueError("Autoscaling nodes ondemand+spot percentage" +
                             " should be 100: Ondemand pct: %d Spot pct: %d"
                             % (autoscaling_ondemand_percentage,
                                autoscaling_spot_percentage))
        if autoscaling_ondemand_percentage > 0:
            autoscaling_nodes.append({"type": "ondemand",
                                      "percentage": autoscaling_ondemand_percentage})
        if autoscaling_spot_percentage > 0:
            autoscaling_nodes.append({"type": "spot",
                                      "percentage": autoscaling_spot_percentage,
                                      "fallback": autoscaling_spot_fallback,
                                      "max_price_percentage": max_price_percentage})
        return composition

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
        network_config_group.add_argument("--master-static-nic-name",
                                          dest="master_static_nic_name",
                                          help="name of NIC to be attached to master node")
        network_config_group.add_argument("--master-static-public-ip-name",
                                          dest="master_static_public_ip_name",
                                          help="name of public IP to be attached to master node")
        network_config_group.add_argument("--resource-group-name",
                                          dest="resource_group_name",
                                          help="resource group for cluster")
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
        storage_config.add_argument("--managed-disk-account-type",
                                    dest="managed_disk_account_type",
                                    default=None,
                                    help="managed disk type for azure cluster")
