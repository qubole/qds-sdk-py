class OracleBmcCloud:

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

        # location settings
        location_group = argparser.add_argument_group("location config settings")
        location_group.add_argument("--oracle-region",
                                    dest="region",
                                    help="region to create the cluster in", )
        location_group.add_argument("--oracle-availability-zone",
                                    dest="availability_domain",
                                    help="availability zone to" +
                                         " create the cluster in", )

        # network settings
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

        # storage config settings
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
                                    help="compute key fingerprint for oracle cluster")
        storage_config.add_argument("--storage-api-private-rsa-key",
                                    dest="storage_api_private_rsa_key",
                                    default=None,
                                    help="storage api private rsa key for oracle cluster")

    def set_cloud_config_settings(self, arguments):
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
                              storage_tenant_id=arguments.storage_tenant_id,
                              storage_user_id=arguments.storage_user_id,
                              storage_key_finger_print=arguments.storage_key_finger_print,
                              storage_api_private_rsa_key=arguments.storage_api_private_rsa_key)

    # write comment
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
                         storage_tenant_id=None,
                         storage_user_id=None,
                         storage_key_finger_print=None,
                         storage_api_private_rsa_key=None):

        def set_compute_config():
            self.compute_config['use_account_compute_creds'] = use_account_compute_creds
            self.compute_config['compute_tenant_id'] = compute_tenant_id
            self.compute_config['compute_user_id'] = compute_user_id
            self.compute_config['compute_key_finger_print'] = compute_key_finger_print
            self.compute_config['compute_api_private_rsa_key'] = compute_api_private_rsa_key

        def set_location():
            self.location['region'] = oracle_region
            self.location['availability_domain'] = oracle_availability_domain

        def set_network_config():
            self.network_config['vcn_id'] = vcn_id
            self.network_config['subnet_id'] = subnet_id
            self.network_config['compartment_id'] = compartment_id
            self.network_config['image_id'] = image_id

        def set_storage_config():
            self.storage_config['storage_tenant_id'] = storage_tenant_id
            self.storage_config['storage_user_id'] = storage_user_id
            self.storage_config['storage_key_finger_print'] = storage_key_finger_print
            self.storage_config['storage_api_private_rsa_key'] = storage_api_private_rsa_key

        set_compute_config()
        set_location()
        set_network_config()
        set_storage_config()