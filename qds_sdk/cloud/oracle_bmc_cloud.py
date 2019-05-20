from qds_sdk.cloud.cloud import Cloud
import json

class OracleBmcCloud(Cloud):
    '''
    qds_sdk.cloud.OracleBmcCloud is the class which stores information about oracle bmc cloud config settings.
    The objects of this class can be used to set oracle_bmc cloud_config settings while create/update/clone a cluster.
    '''

    def __init__(self):
        self.compute_config = {}
        self.location = {}
        self.network_config = {}
        self.storage_config = {}

    def set_cloud_config(self,
                         compute_tenant_id=None,
                         compute_user_id=None,
                         compute_key_finger_print=None,
                         compute_api_private_rsa_key=None,
                         use_account_compute_creds=None,
                         subnet_id=None,
                         oracle_region=None,
                         oracle_availability_domain=None,
                         compartment_id=None,
                         image_id=None,
                         vcn_id=None,
                         availability_domain_info_map=None,
                         storage_tenant_id=None,
                         storage_user_id=None,
                         storage_key_finger_print=None,
                         storage_api_private_rsa_key=None,
                         block_volume_count=None,
                         block_volume_size=None):
        '''

        Args:
            compute_tenant_id: compute tenant id for oracle cluster

            compute_user_id: compute user id for oracle cluster

            compute_key_finger_print: compute key fingerprint for oracle cluster

            compute_api_private_rsa_key: compute api private rsa key for oracle cluster

            use_account_compute_creds: Set it to true to use the account's compute
                credentials for all clusters of the account.The default value is false

            subnet_id: subnet id for oracle

            oracle_region: region to create the cluster in

            oracle_availability_domain: availability zone to create the cluster in

            compartment_id: compartment id for oracle cluster

            image_id: image id for oracle cloud

            vcn_id: vcn to create the cluster in

            availability_domain_info_map: availability domain and subnet mapping for the cluster

            storage_tenant_id: tenant id for oracle cluster

            storage_user_id: storage user id for oracle cluster

            storage_key_finger_print: storage key fingerprint for oracle cluster

            storage_api_private_rsa_key: storage api private rsa key for oracle cluster

            block_volume_count: count of block volumes to be mounted to an instance as reserved disks

            block_volume_size: It is the size (in GB) of each block volume to be mounted to an instance as reserved disk

        '''

        self.set_compute_config(use_account_compute_creds, compute_tenant_id,
                                compute_user_id, compute_key_finger_print,
                                compute_api_private_rsa_key)
        self.set_location(oracle_region, oracle_availability_domain)
        self.set_network_config(vcn_id, subnet_id,
                                compartment_id, image_id, availability_domain_info_map)
        self.set_storage_config(storage_tenant_id, storage_user_id,
                                storage_key_finger_print, storage_api_private_rsa_key, block_volume_count,
                                block_volume_size)

    def  set_compute_config(self,
                            use_account_compute_creds=None,
                            compute_tenant_id=None,
                            compute_user_id=None,
                            compute_key_finger_print=None,
                            compute_api_private_rsa_key=None):
        self.compute_config['use_account_compute_creds'] = use_account_compute_creds
        self.compute_config['compute_tenant_id'] = compute_tenant_id
        self.compute_config['compute_user_id'] = compute_user_id
        self.compute_config['compute_key_finger_print'] = compute_key_finger_print
        self.compute_config['compute_api_private_rsa_key'] = compute_api_private_rsa_key

    def set_location(self,
                     oracle_region=None,
                     oracle_availability_domain=None):
        self.location['region'] = oracle_region
        self.location['availability_domain'] = oracle_availability_domain

    def set_network_config(self,
                           vcn_id=None,
                           subnet_id=None,
                           compartment_id=None,
                           image_id=None,
                           availability_domain_info_map=None):
        self.network_config['vcn_id'] = vcn_id
        self.network_config['subnet_id'] = subnet_id
        self.network_config['compartment_id'] = compartment_id
        self.network_config['image_id'] = image_id
        if availability_domain_info_map and availability_domain_info_map.strip():
            try:
                self.network_config['availability_domain_info_map'] = json.loads(availability_domain_info_map.strip())
            except Exception as e:
                raise Exception("Invalid JSON string for availability domain info map: %s" % e.message)

    def set_storage_config(self,
                           storage_tenant_id=None,
                           storage_user_id=None,
                           storage_key_finger_print=None,
                           storage_api_private_rsa_key=None,
                           block_volume_count=None,
                           block_volume_size=None):
        self.storage_config['storage_tenant_id'] = storage_tenant_id
        self.storage_config['storage_user_id'] = storage_user_id
        self.storage_config['storage_key_finger_print'] = storage_key_finger_print
        self.storage_config['storage_api_private_rsa_key'] = storage_api_private_rsa_key
        self.storage_config['block_volume_count'] = block_volume_count
        self.storage_config['block_volume_size'] = block_volume_size

    def set_cloud_config_from_arguments(self, arguments):
        self.set_cloud_config(compute_tenant_id=arguments.compute_tenant_id,
                              compute_user_id=arguments.compute_user_id,
                              compute_key_finger_print=arguments.compute_key_finger_print,
                              compute_api_private_rsa_key=arguments.compute_api_private_rsa_key,
                              use_account_compute_creds=arguments.use_account_compute_creds,
                              subnet_id=arguments.subnet_id,
                              oracle_region=arguments.region,
                              oracle_availability_domain=arguments.availability_domain,
                              compartment_id=arguments.compartment_id,
                              image_id=arguments.image_id,
                              vcn_id=arguments.vcn_id,
                              availability_domain_info_map=arguments.availability_domain_info_map,
                              storage_tenant_id=arguments.storage_tenant_id,
                              storage_user_id=arguments.storage_user_id,
                              storage_key_finger_print=arguments.storage_key_finger_print,
                              storage_api_private_rsa_key=arguments.storage_api_private_rsa_key,
                              block_volume_count=arguments.block_volume_count,
                              block_volume_size=arguments.block_volume_size
                              )

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
        compute_config.add_argument("--compute-tenant-id",
                                    dest="compute_tenant_id",
                                    default=None,
                                    help="tenant id for oracle cluster")
        compute_config.add_argument("--compute-user-id",
                                    dest="compute_user_id",
                                    default=None,
                                    help="compute user id for oracle cluster")
        compute_config.add_argument("--compute-key-finger-print",
                                    dest="compute_key_finger_print",
                                    default=None,
                                    help="compute key fingerprint for oracle cluster")
        compute_config.add_argument("--compute-api-private-rsa-key",
                                    dest="compute_api_private_rsa_key",
                                    default=None,
                                    help="compute api private rsa key for oracle cluster")

        # location settings parser
        location_group = argparser.add_argument_group("location config settings")
        location_group.add_argument("--oracle-region",
                                    dest="region",
                                    help="region to create the cluster in", )
        location_group.add_argument("--oracle-availability-zone",
                                    dest="availability_domain",
                                    help="availability zone to" +
                                         " create the cluster in", )

        # network settings parser
        network_config_group = argparser.add_argument_group("network config settings")
        network_config_group.add_argument("--compartment-id",
                                          dest="compartment_id",
                                          help="compartment id for oracle cluster")
        network_config_group.add_argument("--image-id",
                                          dest="image_id",
                                          help="image id for oracle cloud")
        network_config_group.add_argument("--vcn-id",
                                          dest="vcn_id",
                                          help="vcn to create the cluster in", )
        network_config_group.add_argument("--subnet-id",
                                          dest="subnet_id",
                                          help="subnet id for oracle")
        network_config_group.add_argument("--availability-domain-info-map",
                                          dest="availability_domain_info_map",
                                          help="availability domain and subnet mapping for the cluster")

        # storage config settings parser
        storage_config = argparser.add_argument_group("storage config settings")
        storage_config.add_argument("--storage-tenant-id",
                                    dest="storage_tenant_id",
                                    default=None,
                                    help="tenant id for oracle cluster")
        storage_config.add_argument("--storage-user-id",
                                    dest="storage_user_id",
                                    default=None,
                                    help="storage user id for oracle cluster")
        storage_config.add_argument("--storage-key-finger-print",
                                    dest="storage_key_finger_print",
                                    default=None,
                                    help="storage key fingerprint for oracle cluster")
        storage_config.add_argument("--storage-api-private-rsa-key",
                                    dest="storage_api_private_rsa_key",
                                    default=None,
                                    help="storage api private rsa key for oracle cluster")
        storage_config.add_argument("--block-volume-count",
                                    dest="block_volume_count",
                                    default=None,
                                    help="count of block volumes to be mounted to an instance as reserved disks",
                                    type=int)
        storage_config.add_argument("--block-volume-size",
                                    dest="block_volume_size",
                                    default=None,
                                    help="size (in GB) of each block volume to be mounted to an instance",
                                    type=int)