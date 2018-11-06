from qds_sdk.cloud.cloud import Cloud
class GcpCloud(Cloud):
    '''
    qds_sdk.cloud.GcpCloud is the class which stores information about gcp cloud config settings.
    The objects of this class can be use to set gcp cloud_config settings while create/update/clone a cluster.
    '''

    def __init__(self):
        self.compute_config = {}
        self.location = {}
        self.network_config = {}
        self.storage_config = {}

    def set_cloud_config(self,
                         compute_client_id=None,
                         compute_project_id=None,
                         compute_client_email=None,
                         compute_private_key_id=None,
                         compute_private_key=None,
                         use_account_compute_creds=None,
                         gcp_region=None,
                         storage_client_id=None,
                         storage_project_id=None,
                         storage_client_email=None,
                         storage_private_key_id=None,
                         storage_private_key=None,
                         bastion_node_public_dns=None,
                         vpc_id=None,
                         subnet_id=None):
        '''

        Args:
            compute_client_id: Compute client id for gcp cluster

            compute_project_id: Compute project id for gcp cluster

            compute_client_email: Compute client email for gcp cluster

            compute_private_key_id: Compute private key id for gcp cluster

            compute_private_key: Compute private key for gcp cluster

            use_account_compute_creds: Set it to true to use the account's compute
                credentials for all clusters of the account.The default value is false

            gcp_region: Region for gcp cluster

            storage_client_id: Storage client id for gcp cluster

            storage_project_id: Storage project id for gcp cluster

            storage_client_email: Storage client email for gcp cluster

            storage_private_key_id: Storage private key id for gcp cluster

            storage_private_key: Storage private key  for gcp cluster

            bastion_node_public_dns: public dns name of the bastion node.
                Required only if cluster is in a private subnet.

            vpc_id: Vpc id for gcp cluster

            subnet_id: Subnet id for gcp cluster
        '''

        self.set_compute_config(use_account_compute_creds, compute_client_id, compute_project_id, compute_client_email,
                                compute_private_key_id, compute_private_key)
        self.set_location(gcp_region)
        self.set_network_config(bastion_node_public_dns, vpc_id, subnet_id)
        self.set_storage_config(storage_client_id, storage_project_id, storage_client_email, storage_private_key_id,
                                storage_private_key)

    def set_compute_config(self,
                           use_account_compute_creds=None,
                           compute_client_id=None,
                           compute_project_id=None,
                           compute_client_email=None,
                           compute_private_key_id=None,
                           compute_private_key=None):
        self.compute_config['use_account_compute_creds'] = use_account_compute_creds
        self.compute_config['compute_client_id'] = compute_client_id
        self.compute_config['compute_project_id'] = compute_project_id
        self.compute_config['compute_client_email'] = compute_client_email
        self.compute_config['compute_private_key_id'] = compute_private_key_id
        self.compute_config['compute_private_key'] = compute_private_key

    def set_location(self,
                     gcp_region=None):
        self.location['region'] = gcp_region

    def set_network_config(self,
                           bastion_node_public_dns=None,
                           vpc_id=None,
                           subnet_id=None):
        self.network_config['bastion_node_public_dns'] = bastion_node_public_dns
        self.network_config['vpc'] = vpc_id
        self.network_config['subnet'] = subnet_id

    def set_storage_config(self,
                           storage_client_id=None,
                           storage_project_id=None,
                           storage_client_email=None,
                           storage_private_key_id=None,
                           storage_private_key=None):
        self.storage_config['storage_client_id'] = storage_client_id
        self.storage_config['storage_project_id'] = storage_project_id
        self.storage_config['storage_client_email'] = storage_client_email
        self.storage_config['storage_private_key_id'] = storage_private_key_id
        self.storage_config['storage_private_key'] = storage_private_key

    def set_cloud_config_from_arguments(self, arguments):
        self.set_cloud_config(compute_client_id=arguments.compute_client_id,
                              compute_project_id=arguments.compute_project_id,
                              compute_client_email=arguments.compute_client_email,
                              compute_private_key_id=arguments.compute_private_key_id,
                              compute_private_key=arguments.compute_private_key,
                              use_account_compute_creds=arguments.use_account_compute_creds,
                              gcp_region=arguments.gcp_region,
                              storage_client_id=arguments.storage_client_id,
                              storage_project_id=arguments.storage_project_id,
                              storage_client_email=arguments.storage_client_email,
                              storage_private_key_id=arguments.storage_private_key_id,
                              storage_private_key=arguments.storage_private_key,
                              bastion_node_public_dns=arguments.bastion_node_public_dns,
                              vpc_id=arguments.vpc_id,
                              subnet_id=arguments.subnet_id)

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
                                    help="compute client id for gcp cluster")
        compute_config.add_argument("--compute-project-id",
                                    dest="compute_project_id",
                                    default=None,
                                    help="compute project id for gcp cluster")
        compute_config.add_argument("--compute-client-email",
                                    dest="compute_client_email",
                                    default=None,
                                    help="compute client email for gcp cluster")
        compute_config.add_argument("--compute-private-key-id",
                                    dest="compute_private_key_id",
                                    default=None,
                                    help="compute private key id for gcp cluster")
        compute_config.add_argument("--compute-private-key",
                                    dest="compute_private_key",
                                    default=None,
                                    help="compute private key for gcp cluster")

        # location settings parser
        location_group = argparser.add_argument_group("location config settings")
        location_group.add_argument("--gcp-region",
                                    dest="gcp_region",
                                    help="region to create the cluster in")

        # network settings parser
        network_config_group = argparser.add_argument_group("network config settings")
        network_config_group.add_argument("--bastion-node-public-dns",
                                          dest="bastion_node_public_dns",
                                          help="public dns name of the bastion node. Required only if cluster is in private subnet")
        network_config_group.add_argument("--vpc-id",
                                          dest="vpc_id",
                                          help="vpc id to create the cluster in")
        network_config_group.add_argument("--subnet-id",
                                          dest="subnet_id",
                                          help="subnet id to create the cluster in")

        # storage config settings parser
        storage_config = argparser.add_argument_group("storage config settings")
        storage_config.add_argument("--storage-client-id",
                                    dest="storage_client_id",
                                    default=None,
                                    help="storage client id for gcp cluster")
        storage_config.add_argument("--storage-project-id",
                                    dest="storage_project_id",
                                    default=None,
                                    help="storage project id for gcp cluster")
        storage_config.add_argument("--storage-client-email",
                                    dest="storage_client_email",
                                    default=None,
                                    help="storage client email for gcp cluster")
        storage_config.add_argument("--storage-private-key-id",
                                    dest="storage_private_key_id",
                                    default=None,
                                    help="storage private key id for gcp cluster")
        storage_config.add_argument("--storage-private-key",
                                    dest="storage_private_key",
                                    default=None,
                                    help="storage private key for gcp cluster")