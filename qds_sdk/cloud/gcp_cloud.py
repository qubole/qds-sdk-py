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
        self.cluster_composition = {}

    def set_cloud_config(self,
                         qsa_client_id=None,
                         customer_project_id=None,
                         qsa_client_email=None,
                         qsa_private_key_id=None,
                         qsa_private_key=None,
                         comp_client_email=None,
                         inst_client_email=None,
                         use_account_compute_creds=None,
                         gcp_region=None,
                         gcp_zone=None,
                         storage_disk_size_in_gb=None,
                         storage_disk_count=None,
                         storage_disk_type=None,
                         bastion_node_public_dns=None,
                         vpc_id=None,
                         subnet_id=None,
                         master_preemptible=None,
                         min_nodes_preemptible=None,
                         min_nodes_preemptible_percentage=None,
                         autoscaling_nodes_preemptible=None,
                         autoscaling_nodes_preemptible_percentage=None):
        '''

        Args:
            qsa_client_id: Compute client id for gcp cluster

            customer_project_id: Compute project id for gcp cluster

            qsa_client_email: Compute client email for gcp cluster

            qsa_private_key_id: Compute private key id for gcp cluster

            qsa_private_key: Compute private key for gcp cluster

            comp_client_email: Client compute service account email
            
            inst_client_email: Client storage/instance service account email

            use_account_compute_creds: Set it to true to use the account's compute
                credentials for all clusters of the account.The default value is false

            gcp_region: Region for gcp cluster

            bastion_node_public_dns: public dns name of the bastion node.
                Required only if cluster is in a private subnet.

            vpc_id: Vpc id for gcp cluster

            subnet_id: Subnet id for gcp cluster

            master_preemptible: if the master node is preemptible

            min_nodes_preemptible: if the min nodes are preemptible

            min_nodes_preemptible_percentage: percentage of min nodes that are preemptible

            autoscaling_nodes_preemptible: if the autoscaling nodes are preemptible

            autoscaling_nodes_preemptible_percentage: percentage of autoscaling nodes that are preemptible
        '''

        self.set_compute_config(use_account_compute_creds, qsa_client_id, customer_project_id, qsa_client_email,
                                qsa_private_key_id, qsa_private_key, comp_client_email)
        self.set_location(gcp_region, gcp_zone)
        self.set_network_config(bastion_node_public_dns, vpc_id, subnet_id)
        self.set_storage_config(inst_client_email, storage_disk_size_in_gb, storage_disk_count, storage_disk_type)
        self.set_cluster_composition(master_preemptible, min_nodes_preemptible, min_nodes_preemptible_percentage,
                                     autoscaling_nodes_preemptible, autoscaling_nodes_preemptible_percentage)

    def set_compute_config(self,
                           use_account_compute_creds=None,
                           qsa_client_id=None,
                           customer_project_id=None,
                           qsa_client_email=None,
                           qsa_private_key_id=None,
                           qsa_private_key=None,
                           comp_client_email=None):
        self.compute_config['use_account_compute_creds'] = use_account_compute_creds
        self.compute_config['qsa_client_id'] = qsa_client_id
        self.compute_config['customer_project_id'] = customer_project_id
        self.compute_config['qsa_client_email'] = qsa_client_email
        self.compute_config['qsa_private_key_id'] = qsa_private_key_id
        self.compute_config['qsa_private_key'] = qsa_private_key
        self.compute_config['comp_client_email'] = comp_client_email

    def set_location(self,
                     gcp_region=None,
                     gcp_zone=None,
                     ):
        self.location['region'] = gcp_region
        self.location['zone'] = gcp_zone

    def set_network_config(self,
                           bastion_node_public_dns=None,
                           vpc_id=None,
                           subnet_id=None):
        self.network_config['bastion_node_public_dns'] = bastion_node_public_dns
        self.network_config['network'] = vpc_id
        self.network_config['subnet'] = subnet_id

    def set_storage_config(self,
                           inst_client_email=None,
                           storage_disk_size_in_gb=None,
                           storage_disk_count=None,
                           storage_disk_type=None
                           ):
        self.storage_config['inst_client_email'] = inst_client_email
        self.storage_config['disk_size_in_gb'] = storage_disk_size_in_gb
        self.storage_config['disk_count'] = storage_disk_count
        self.storage_config['disk_type'] = storage_disk_type

    def set_cluster_composition(self,
                                master_preemptible=None,
                                min_nodes_preemptible=None,
                                min_nodes_preemptible_percentage=None,
                                autoscaling_nodes_preemptible=None,
                                autoscaling_nodes_preemptible_percentage=None):
        self.cluster_composition['master'] = {}
        self.cluster_composition['master']['preemptible'] = master_preemptible
        self.cluster_composition['min_nodes'] = {}
        self.cluster_composition['min_nodes']['preemptible'] = min_nodes_preemptible
        self.cluster_composition['min_nodes']['percentage'] = min_nodes_preemptible_percentage
        self.cluster_composition['autoscaling_nodes'] = {}
        self.cluster_composition['autoscaling_nodes']['preemptible'] = autoscaling_nodes_preemptible
        self.cluster_composition['autoscaling_nodes']['percentage'] = autoscaling_nodes_preemptible_percentage

    def set_cloud_config_from_arguments(self, arguments):
        self.set_cloud_config(qsa_client_id=arguments.qsa_client_id,
                              customer_project_id=arguments.customer_project_id,
                              qsa_client_email=arguments.qsa_client_email,
                              qsa_private_key_id=arguments.qsa_private_key_id,
                              qsa_private_key=arguments.qsa_private_key,
                              inst_client_email=arguments.inst_client_email,
                              comp_client_email=arguments.comp_client_email,
                              use_account_compute_creds=arguments.use_account_compute_creds,
                              gcp_region=arguments.gcp_region,
                              gcp_zone=arguments.gcp_zone,
                              storage_disk_size_in_gb=arguments.storage_disk_size_in_gb,
                              storage_disk_count=arguments.storage_disk_count,
                              storage_disk_type=arguments.storage_disk_type,
                              bastion_node_public_dns=arguments.bastion_node_public_dns,
                              vpc_id=arguments.vpc_id,
                              subnet_id=arguments.subnet_id,
                              master_preemptible=arguments.master_preemptible,
                              min_nodes_preemptible=arguments.min_nodes_preemptible,
                              min_nodes_preemptible_percentage=arguments.min_nodes_preemptible_percentage,
                              autoscaling_nodes_preemptible=arguments.autoscaling_nodes_preemptible,
                              autoscaling_nodes_preemptible_percentage=arguments.autoscaling_nodes_preemptible_percentage)

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
        compute_config.add_argument("--qsa-client-id",
                                    dest="qsa_client_id",
                                    default=None,
                                    help="qsa client id for gcp cluster")
        compute_config.add_argument("--customer-project-id",
                                    dest="customer_project_id",
                                    default=None,
                                    help="customer project id for gcp cluster")
        compute_config.add_argument("--qsa-client-email",
                                    dest="qsa_client_email",
                                    default=None,
                                    help="qsa client email for gcp cluster")
        compute_config.add_argument("--qsa-private-key-id",
                                    dest="qsa_private_key_id",
                                    default=None,
                                    help="qsa private key id for gcp cluster")
        compute_config.add_argument("--qsa-private-key",
                                    dest="qsa_private_key",
                                    default=None,
                                    help="qsa private key for gcp cluster")
        compute_config.add_argument("--compute-client-email",
                                    dest="comp_client_email",
                                    default=None,
                                    help="client compute service account email")

        # location settings parser
        location_group = argparser.add_argument_group("location config settings")
        location_group.add_argument("--gcp-region",
                                    dest="gcp_region",
                                    help="region to create the cluster in")
        location_group.add_argument("--gcp-zone",
                                    dest="gcp_zone",
                                    help="zone to create the cluster in")

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

        storage_config.add_argument("--storage-client-email",
                                    dest="inst_client_email",
                                    default=None,
                                    help="client storage service account email")
        storage_config.add_argument("--storage-disk-size-in-gb",
                                    dest="storage_disk_size_in_gb",
                                    default=None,
                                    help="disk size in gb for gcp cluster")
        storage_config.add_argument("--storage-disk-count",
                                    dest="storage_disk_count",
                                    default=None,
                                    help="disk count for gcp cluster")
        storage_config.add_argument("--storage-disk-type",
                                    dest="storage_disk_type",
                                    default=None,
                                    help="disk type for gcp cluster")
        # cluster composition settings parser
        cluster_composition = argparser.add_argument_group("cluster composition settings")
        cluster_composition.add_argument("--master-preemptible",
                                         dest="master_preemptible",
                                         action="store_true",
                                         default=None,
                                         help="if the master node is preemptible")
        cluster_composition.add_argument("--min-nodes-preemptible",
                                         dest="min_nodes_preemptible",
                                         action="store_true",
                                         default=None,
                                         help="if the min nodes are preemptible")
        cluster_composition.add_argument("--min-nodes-preemptible-percentage",
                                         dest="min_nodes_preemptible_percentage",
                                         type=int,
                                         default=None,
                                         help="percentage of min nodes that are preemptible")
        cluster_composition.add_argument("--autoscaling-nodes-preemptible",
                                         dest="autoscaling_nodes_preemptible",
                                         action="store_true",
                                         default=None,
                                         help="if the autoscaling nodes are preemptible")
        cluster_composition.add_argument("--autoscaling-nodes-preemptible-percentage",
                                         dest="autoscaling_nodes_preemptible_percentage",
                                         type=int,
                                         default=None,
                                         help="percentage of autoscaling nodes that are preemptible")
