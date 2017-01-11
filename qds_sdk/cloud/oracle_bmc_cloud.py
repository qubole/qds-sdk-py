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








